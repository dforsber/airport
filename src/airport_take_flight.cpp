#include "airport_extension.hpp"
#include "duckdb.hpp"

// Arrow includes.
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>
#include <arrow/util/uri.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/filesystem/localfs.h>

#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "airport_flight_exception.hpp"
#include "airport_flight_statistics.hpp"
#include "airport_flight_stream.hpp"
#include "airport_json_common.hpp"
#include "airport_json_serializer.hpp"
#include "airport_macros.hpp"
#include "airport_request_headers.hpp"
#include "airport_schema_utils.hpp"
#include "airport_take_flight.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "msgpack.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_table_entry.hpp"
#include <openssl/bio.h>
#include <openssl/evp.h>

namespace duckdb
{
  // Create a FlightDescriptor from a DuckDB value which can be one of a few different
  // types.
  static flight::FlightDescriptor flight_descriptor_from_value(duckdb::Value &flight_descriptor)
  {
    switch (flight_descriptor.type().id())
    {
    case LogicalTypeId::BLOB:
    case LogicalTypeId::VARCHAR:
      return flight::FlightDescriptor::Command(flight_descriptor.ToString());
    case LogicalTypeId::LIST:
    {
      auto &list_values = ListValue::GetChildren(flight_descriptor);
      vector<string> components;
      for (idx_t i = 0; i < list_values.size(); i++)
      {
        auto &child = list_values[i];
        if (child.type().id() != LogicalTypeId::VARCHAR)
        {
          throw InvalidInputException("airport_take_flight: when specifying a path all list components must be a varchar");
        }
        components.emplace_back(child.ToString());
      }
      return flight::FlightDescriptor::Path(components);
    }
    case LogicalTypeId::ARRAY:
    {
      auto &array_values = ArrayValue::GetChildren(flight_descriptor);
      vector<string> components;
      for (idx_t i = 0; i < array_values.size(); i++)
      {
        auto &child = array_values[i];
        if (child.type().id() != LogicalTypeId::VARCHAR)
        {
          throw InvalidInputException("airport_take_flight: when specifying a path all list components must be a varchar");
        }
        components.emplace_back(child.ToString());
      }
      return flight::FlightDescriptor::Path(components);
    }
    // FIXME: deal with the union type returned by Arrow list flights.
    default:
      throw InvalidInputException("airport_take_flight: unknown descriptor type passed");
    }
  }

  unique_ptr<FunctionData>
  AirportTakeFlightBindWithFlightDescriptor(
      const AirportTakeFlightParameters &take_flight_params,
      const flight::FlightDescriptor &descriptor,
      ClientContext &context,
      const TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names,
      // So rather than the cached_flight_info_ptr here we can just have the cached schema.
      std::shared_ptr<arrow::Schema> schema,
      const std::optional<AirportTableFunctionFlightInfoParameters> &table_function_parameters,
      const AirportTableEntry *table_entry)
  {
    // Create a UID for tracing.
    const auto trace_uuid = airport_trace_id();

    auto &server_location = take_flight_params.server_location();

    // The main thing that needs to be answered in this function
    // are the names and return types and establishing the bind data.
    //
    // If the cached_flight_info_ptr is not null we should use the schema
    // from that flight info otherwise it should be requested.

    arrow::flight::FlightCallOptions call_options;
    airport_add_normal_headers(call_options, take_flight_params, trace_uuid,
                               descriptor);

    int64_t estimated_records = -1;

    // If we are applying time travel, the schema that we have is the latest schema
    // but back in time the schema may have been different.
    //
    // We should just request the schema from the server if we are using time travel.
    // Additionally the number of estimated records may also be different.

    if (!take_flight_params.at_unit().empty() && !take_flight_params.at_value().empty())
    {
      schema = nullptr;
    }

    // Get the information about the flight, this will allow the
    // endpoint information to be returned.

    if (schema == nullptr)
    {
      std::unique_ptr<arrow::flight::FlightInfo> retrieved_flight_info;
      auto flight_client = AirportAPI::FlightClientForLocation(server_location);

      if (table_function_parameters != std::nullopt)
      {
        // Rather than calling GetFlightInfo we will call DoAction and get
        // get the flight info that way, since it allows us to serialize
        // all of the data we need to send instead of just the flight name.

        AirportTableFunctionFlightInfoParameters augmented_parameters(*table_function_parameters);

        augmented_parameters.at_unit = take_flight_params.at_unit();
        augmented_parameters.at_value = take_flight_params.at_value();
        AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "table_function_flight_info", augmented_parameters);

        AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto action_results, flight_client->DoAction(call_options, action), server_location, "airport_table_function_flight_info");

        // The only item returned is a serialized flight info.
        AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto serialized_flight_info_buffer, action_results->Next(), server_location, "reading get_flight_info for table function");

        std::string_view serialized_flight_info(reinterpret_cast<const char *>(serialized_flight_info_buffer->body->data()), serialized_flight_info_buffer->body->size());

        // Now deserialize that flight info so we can use it.
        AIRPORT_ASSIGN_OR_RAISE_LOCATION(retrieved_flight_info, arrow::flight::FlightInfo::Deserialize(serialized_flight_info), server_location, "deserialize flight info");

        AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), server_location, "");
      }
      else if (table_entry != nullptr)
      {
        // We have a table entry, which means this isn't an adhoc call to airport_take_flight, so we can call
        // the flight_info action rather than GetFlightInfo which allows additional parameters to be passed.
        AirportFlightInfoParameters get_flight_info_params;

        AIRPORT_ASSIGN_OR_RAISE_LOCATION(
            get_flight_info_params.descriptor,
            descriptor.SerializeToString(),
            server_location,
            "airport_take_flight: serialize flight descriptor");

        get_flight_info_params.at_unit = take_flight_params.at_unit();
        get_flight_info_params.at_value = take_flight_params.at_value();

        AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "flight_info", get_flight_info_params);

        AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto action_results, flight_client->DoAction(call_options, action), server_location, "airport_table_function_flight_info");

        // The only item returned is a serialized flight info.
        AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto serialized_flight_info_buffer, action_results->Next(), server_location, "reading flight_info for flight");

        std::string_view serialized_flight_info(reinterpret_cast<const char *>(serialized_flight_info_buffer->body->data()), serialized_flight_info_buffer->body->size());

        // Now deserialize that flight info so we can use it.
        AIRPORT_ASSIGN_OR_RAISE_LOCATION(retrieved_flight_info, arrow::flight::FlightInfo::Deserialize(serialized_flight_info), server_location, "deserialize flight info");

        AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), server_location, "");
      }
      else
      {
        AIRPORT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(retrieved_flight_info,
                                                    flight_client->GetFlightInfo(call_options, descriptor),
                                                    server_location,
                                                    descriptor,
                                                    "");
      }

      // Assert that the descriptor is the same as the one that was passed in.
      if (descriptor != retrieved_flight_info->descriptor())
      {
        throw InvalidInputException("airport_take_flight: descriptor returned from server does not match the descriptor that was passed in to GetFlightInfo, check with Flight server implementation.");
      }

      estimated_records = retrieved_flight_info->total_records();

      arrow::ipc::DictionaryMemo dictionary_memo;
      AIRPORT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(schema,
                                                  retrieved_flight_info->GetSchema(&dictionary_memo),
                                                  server_location,
                                                  descriptor,
                                                  "");
    }

    auto ret = make_uniq<AirportTakeFlightBindData>(
        (stream_factory_produce_t)&AirportCreateStream,
        trace_uuid,
        estimated_records,
        take_flight_params,
        table_function_parameters,
        schema,
        descriptor,
        table_entry,
        nullptr);

    AirportExamineSchema(context,
                         ret->schema_root,
                         &ret->arrow_table,
                         &return_types,
                         &names,
                         nullptr,
                         &ret->rowid_column_index,
                         true);

    // Store the return types and names so they can be
    // validated by parquet_scans or other scans used in endpoints.
    ret->set_types_and_names(return_types, names);

    return ret;
  }

  static unique_ptr<FunctionData> take_flight_bind(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    auto server_location = input.inputs[0].ToString();
    AirportTakeFlightParameters params(server_location, context, input);
    auto descriptor = flight_descriptor_from_value(input.inputs[1]);

    return AirportTakeFlightBindWithFlightDescriptor(
        params,
        descriptor,
        context,
        input, return_types, names, nullptr, std::nullopt, nullptr);
  }

  static unique_ptr<FunctionData> take_flight_bind_with_pointer(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    if (input.inputs[0].IsNull())
    {
      throw BinderException("airport: take_flight_with_pointer, pointers to AirportTable cannot be null");
    }

    if (input.inputs[1].IsNull())
    {
      throw BinderException("airport: take_flight_with_pointer, pointers to AirportTable cannot be null");
    }

    const auto info = reinterpret_cast<const duckdb::AirportAPITable *>(input.inputs[0].GetPointer());
    const auto table_entry = reinterpret_cast<const AirportTableEntry *>(input.inputs[1].GetPointer());

    AirportTakeFlightParameters params(info->server_location(), context, input);

    // The transaction identifier is passed as the 2nd argument.
    if (!input.inputs[2].IsNull())
    {
      auto id = input.inputs[2].ToString();
      if (!id.empty())
      {
        params.add_header("airport-transaction-id", id);
      }
    }

    return AirportTakeFlightBindWithFlightDescriptor(
        params,
        info->descriptor(),
        context,
        input,
        return_types,
        names,
        info->schema(),
        std::nullopt,
        table_entry);
  }

  static bool
  AirportLocalStateProcessEndpoint(ClientContext &context,
                                   const TableFunctionInitInput &input,
                                   const AirportTakeFlightBindData &bind_data,
                                   AirportArrowScanGlobalState &global_state,
                                   AirportArrowScanLocalState &local_state,
                                   const flight::FlightEndpoint endpoint);

  static bool AirportArrowScanParallelStateNext(AirportArrowScanLocalState &state,
                                                AirportArrowScanGlobalState &global_state,
                                                const AirportTakeFlightBindData &bind_data,
                                                ClientContext &context)
  {
    if (state.done)
    {
      return false;
    }
    state.Reset();
    state.batch_index++; //= ++parallel_state.batch_index;

    bool finished_chunk = false;
    auto &reader = state.reader();
    if (std::holds_alternative<std::shared_ptr<AirportLocalScanData>>(reader))
    {
      auto &scan_data = std::get<std::shared_ptr<AirportLocalScanData>>(reader);
      if (scan_data->finished_chunk)
      {
        finished_chunk = true;
      }
    }
    else
    {
      auto current_chunk = state.stream()->GetNextChunk();
      while (current_chunk->arrow_array.length == 0 && current_chunk->arrow_array.release)
      {
        current_chunk = state.stream()->GetNextChunk();
      }
      state.chunk = std::move(current_chunk);

      finished_chunk = !state.chunk->arrow_array.release;
    }

    //! have we run out of chunks? we are done
    if (finished_chunk)
    {
      auto &endpoint_opt = global_state.GetNextEndpoint();
      if (endpoint_opt)
      {
        if (AirportLocalStateProcessEndpoint(context,
                                             state.input(),
                                             bind_data,
                                             global_state,
                                             state,
                                             *endpoint_opt))
        {
          return true;
        }
      }
      state.done = true;
      return false;
    }
    return true;
  }

  static void AirportDataFromLocalScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
  {
    auto &state = data_p.local_state->Cast<AirportArrowScanLocalState>();

    auto &reader = state.reader();

    D_ASSERT(std::holds_alternative<std::shared_ptr<AirportLocalScanData>>(reader));

    auto &scan_data = std::get<std::shared_ptr<AirportLocalScanData>>(reader);

    TableFunctionInput function_input(scan_data->bind_data.get(),
                                      scan_data->local_state.get(),
                                      scan_data->global_state.get());
    scan_data->table_function.function(context, function_input, output);

    for (auto &idx : scan_data->not_mapped_column_indexes)
    {
      auto &vec = output.data[idx];
      vec.SetVectorType(VectorType::FLAT_VECTOR);
      FlatVector::Validity(vec).SetAllInvalid(output.size());
    }

    auto count = output.size();
    scan_data->finished_chunk = count == 0;
    output.Verify();
  }

  static void AirportDataFromStream(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
  {
    auto &state = data_p.local_state->Cast<AirportArrowScanLocalState>();
    auto &global_state = data_p.global_state->Cast<AirportArrowScanGlobalState>();
    auto &airport_bind_data = data_p.bind_data->CastNoConst<AirportTakeFlightBindData>();
    auto &reader = state.reader();

    D_ASSERT(!std::holds_alternative<std::shared_ptr<AirportLocalScanData>>(reader));

    const auto &array_length = (idx_t)state.chunk->arrow_array.length;

    const auto output_size =
        MinValue<int64_t>(STANDARD_VECTOR_SIZE,
                          array_length - state.chunk_offset);

    // lines_read needs to be set on the local state rather than the bind state.

    state.lines_read += output_size;

    if (global_state.CanRemoveFilterColumns())
    {
      // So state.all_columns is a smaller DataChunk, that
      // should just contain the number of columns taht are in the all
      // columns vector.
      state.all_columns.Reset();
      state.all_columns.SetCardinality(output_size);
      if (output_size > 0)
      {
        ArrowTableFunction::ArrowToDuckDB(state,
                                          airport_bind_data.arrow_table.GetColumns(),
                                          state.all_columns,
                                          state.lines_read - output_size,
                                          false,
                                          airport_bind_data.rowid_column_index);
      }
      output.ReferenceColumns(state.all_columns, global_state.projection_ids());
    }
    else
    {
      output.SetCardinality(output_size);
      if (output_size > 0)
      {
        ArrowTableFunction::ArrowToDuckDB(state,
                                          airport_bind_data.arrow_table.GetColumns(),
                                          output,
                                          state.lines_read - output_size,
                                          false,
                                          airport_bind_data.rowid_column_index);
      }
    }

    state.chunk_offset += output.size();
    output.Verify();
  }

  void AirportTakeFlight(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
  {
    // All of these should be defined.
    D_ASSERT(data_p.local_state);
    D_ASSERT(data_p.global_state);
    D_ASSERT(data_p.bind_data);
    auto &state = data_p.local_state->Cast<AirportArrowScanLocalState>();
    auto &global_state = data_p.global_state->Cast<AirportArrowScanGlobalState>();
    auto &airport_bind_data = data_p.bind_data->CastNoConst<AirportTakeFlightBindData>();

    while (true)
    {
      auto &reader = state.reader();
      const auto has_local_scan = std::holds_alternative<std::shared_ptr<AirportLocalScanData>>(reader);
      if (has_local_scan)
      {
        AirportDataFromLocalScanFunction(context, data_p, output);
      }
      else
      {
        AirportDataFromStream(context, data_p, output);
      }

      if (output.size() != 0)
      {
        break;
      }

      if (!AirportArrowScanParallelStateNext(state,
                                             global_state,
                                             airport_bind_data,
                                             context))
      {
        break;
      }
    }
  }

  unique_ptr<NodeStatistics> AirportTakeFlightCardinality(ClientContext &context, const FunctionData *data)
  {
    // To estimate the cardinality of the flight, we can peek at the flight information
    // that was retrieved during the bind function.
    //
    // This estimate does not take into account any filters that may have been applied
    //
    auto &bind_data = data->Cast<AirportTakeFlightBindData>();
    auto flight_estimated_records = bind_data.estimated_records();

    if (flight_estimated_records != -1)
    {
      return make_uniq<NodeStatistics>(flight_estimated_records);
    }
    // If we don't have an estimated number of records, just use an assumption.
    return make_uniq<NodeStatistics>(100000);
  }

  void AirportTakeFlightComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                              vector<unique_ptr<Expression>> &filters)
  {
    auto allocator = AirportJSONAllocator(BufferAllocator::Get(context));

    auto alc = allocator.GetYYAlc();

    auto doc = AirportJSONCommon::CreateDocument(alc);
    auto result_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, result_obj);

    auto filters_arr = yyjson_mut_arr(doc);

    for (auto &f : filters)
    {
      auto serializer = AirportJsonSerializer(doc, false, false, false);
      f->Serialize(serializer);
      yyjson_mut_arr_append(filters_arr, serializer.GetRootObject());
    }

    yyjson_mut_val *column_id_names = yyjson_mut_arr(doc);
    for (auto id : get.GetColumnIds())
    {
      // So there can be a special column id specified called rowid.
      yyjson_mut_arr_add_str(doc, column_id_names, id.IsRowIdColumn() ? "rowid" : get.names[id.GetPrimaryIndex()].c_str());
    }

    yyjson_mut_obj_add_val(doc, result_obj, "filters", filters_arr);
    yyjson_mut_obj_add_val(doc, result_obj, "column_binding_names_by_index", column_id_names);
    idx_t len;
    auto data = yyjson_mut_val_write_opts(
        result_obj,
        AirportJSONCommon::WRITE_FLAG,
        alc, reinterpret_cast<size_t *>(&len), nullptr);

    if (data == nullptr)
    {
      throw SerializationException(
          "Failed to serialize json, perhaps the query contains invalid utf8 characters?");
    }

    auto json_result = string(data, (size_t)len);

    auto &bind_data = bind_data_p->Cast<AirportTakeFlightBindData>();

    bind_data.json_filters = json_result;
  }

  shared_ptr<ArrowArrayStreamWrapper> AirportProduceArrowScan(
      const ArrowScanFunctionData &function,
      const vector<column_t> &column_ids,
      const TableFilterSet *filters,
      atomic<double> *progress,
      std::shared_ptr<arrow::Buffer> *last_app_metadata,
      const std::shared_ptr<arrow::Schema> &schema,
      const AirportLocationDescriptor &location_descriptor,
      AirportArrowScanLocalState &local_state)
  {
    AirportArrowStreamParameters parameters(progress,
                                            last_app_metadata,
                                            schema,
                                            location_descriptor);

    auto &projected = parameters.projected_columns;
    // Preallocate space for efficiency
    projected.columns.reserve(column_ids.size());
    projected.projection_map.reserve(column_ids.size());
    projected.filter_to_col.reserve(column_ids.size());

    for (const auto col_idx : column_ids)
    {
      if (col_idx == COLUMN_IDENTIFIER_ROW_ID)
        continue;

      const auto &schema = *function.schema_root.arrow_schema.children[col_idx];
      projected.projection_map.emplace(col_idx, schema.name);
      projected.columns.emplace_back(schema.name);
      projected.filter_to_col.emplace(col_idx, col_idx);
    }

    parameters.filters = (TableFilterSet *)filters;

    return function.scanner_producer((uintptr_t)&local_state, parameters);
  }

  // static string CompressString(const string &input, const string &location, const flight::FlightDescriptor &descriptor)
  // {
  //   auto codec = arrow::util::Codec::Create(arrow::Compression::ZSTD, 1).ValueOrDie();

  //   // Estimate the maximum compressed size (usually larger than original size)
  //   int64_t max_compressed_len = codec->MaxCompressedLen(input.size(), reinterpret_cast<const uint8_t *>(input.data()));

  //   // Allocate a buffer to hold the compressed data

  //   AIRPORT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto compressed_buffer, arrow::AllocateBuffer(max_compressed_len), location, descriptor, "");

  //   // Perform the compression
  //   AIRPORT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto compressed_size,
  //                                                      codec->Compress(
  //                                                          input.size(),
  //                                                          reinterpret_cast<const uint8_t *>(input.data()),
  //                                                          max_compressed_len,
  //                                                          compressed_buffer->mutable_data()),
  //                                                      location, descriptor, "");

  //   // If you want to write the compressed data to a string
  //   std::string compressed_str(reinterpret_cast<const char *>(compressed_buffer->data()), compressed_size);
  //   return compressed_str;
  // }

  struct AirportEndpointParameters
  {
    std::string json_filters;
    std::vector<idx_t> column_ids;

    // The parameters to the table function, which should
    // be included in the opaque ticket data returned
    // for each endpoint.
    std::string table_function_parameters;
    std::string table_function_input_schema;

    std::string at_unit;
    std::string at_value;

    MSGPACK_DEFINE_MAP(json_filters, column_ids, table_function_parameters, table_function_input_schema, at_unit, at_value)
  };

  // static string BuildCompressedTicketMetadata(const string &json_filters, const vector<idx_t> &column_ids, uint32_t *uncompressed_length, const string &location, const flight::FlightDescriptor &descriptor)
  // {
  //   AirportTicketMetadataParameters params;
  //   params.json_filters = json_filters;
  //   params.column_ids = column_ids;

  //   std::stringstream packed_buffer;
  //   msgpack::pack(packed_buffer, params);

  //   auto metadata_doc_string = packed_buffer.str();
  //   *uncompressed_length = metadata_doc_string.size();
  //   auto compressed_metadata = CompressString(metadata_doc_string, location, descriptor);

  //   return compressed_metadata;
  // }

  struct AirportGetFlightEndpointsRequest
  {
    std::string descriptor;
    AirportEndpointParameters parameters;

    MSGPACK_DEFINE_MAP(descriptor, parameters)
  };

  static vector<flight::FlightEndpoint> AirportGetFlightEndpoints(
      const AirportTakeFlightParameters &take_flight_params,
      const string &trace_id,
      const flight::FlightDescriptor &descriptor,
      const std::shared_ptr<flight::FlightClient> &flight_client,
      const std::string &json_filters,
      const vector<idx_t> &column_ids,
      const std::string &table_function_parameters,
      const std::string &table_function_input_schema)
  {
    vector<flight::FlightEndpoint> endpoints;
    arrow::flight::FlightCallOptions call_options;
    auto &server_location = take_flight_params.server_location();

    airport_add_normal_headers(call_options, take_flight_params, trace_id,
                               descriptor);

    AirportGetFlightEndpointsRequest endpoints_request;

    AIRPORT_ASSIGN_OR_RAISE_LOCATION(
        endpoints_request.descriptor,
        descriptor.SerializeToString(),
        server_location,
        "endpoints serialize flight descriptor");

    endpoints_request.parameters.json_filters = json_filters;
    endpoints_request.parameters.column_ids = column_ids;
    endpoints_request.parameters.table_function_parameters = table_function_parameters;
    endpoints_request.parameters.table_function_input_schema = table_function_input_schema;
    endpoints_request.parameters.at_unit = take_flight_params.at_unit();
    endpoints_request.parameters.at_value = take_flight_params.at_value();

    AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "endpoints", endpoints_request);

    AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto action_results,
                                     flight_client->DoAction(call_options, action),
                                     server_location,
                                     "airport endpoints action");

    // The only item returned is a serialized flight info.
    AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto serialized_endpoint_info_buffer,
                                     action_results->Next(),
                                     server_location,
                                     "reading endpoints");

    std::string_view serialized_endpoint_info(reinterpret_cast<const char *>(serialized_endpoint_info_buffer->body->data()), serialized_endpoint_info_buffer->body->size());

    AIRPORT_MSGPACK_UNPACK(std::vector<std::string>,
                           serialized_endpoints,
                           serialized_endpoint_info,
                           server_location,
                           "File to parse msgpack encoded endpoints");

    AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), server_location, "endpoints drain");

    endpoints.reserve(serialized_endpoints.size());

    for (const auto &endpoint : serialized_endpoints)
    {
      AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto deserialized_endpoint,
                                       arrow::flight::FlightEndpoint::Deserialize(endpoint),
                                       server_location,
                                       "deserialize flight endpoint");
      endpoints.push_back(std::move(deserialized_endpoint));
    }
    return endpoints;
  }

  unique_ptr<GlobalTableFunctionState> AirportArrowScanInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input)
  {
    auto &bind_data = input.bind_data->CastNoConst<AirportTakeFlightBindData>();

    // Ideally this is where we call GetFlightInfo to obtain the endpoints, but
    // GetFlightInfo can't take the predicate information, so we'll need to call an
    // action called endpoints.
    //
    // FIXME: somehow the flight should be marked if it supports predicate pushdown.
    // right now I'm not sure what this is.
    //
    auto flight_client = AirportAPI::FlightClientForLocation(bind_data.server_location());

    vector<idx_t> projection_ids;
    vector<LogicalType> scanned_types;

    if (!input.projection_ids.empty())
    {
      projection_ids = input.projection_ids;
      for (const auto &col_idx : input.column_ids)
      {
        if (col_idx == COLUMN_IDENTIFIER_ROW_ID)
        {
          auto rowid_type = AirportAPI::GetRowIdType(
              context,
              bind_data.schema(),
              bind_data);
          scanned_types.emplace_back(rowid_type);
        }
        else
        {
          scanned_types.push_back(bind_data.all_types[col_idx]);
        }
      }
    }

    auto result = make_uniq<AirportArrowScanGlobalState>(
        AirportGetFlightEndpoints(bind_data.take_flight_params(),
                                  bind_data.trace_id(),
                                  bind_data.descriptor(),
                                  flight_client,
                                  bind_data.json_filters,
                                  input.column_ids,
                                  bind_data.table_function_parameters().has_value() ? bind_data.table_function_parameters()->parameters : "",
                                  bind_data.table_function_parameters().has_value() ? bind_data.table_function_parameters()->table_input_schema : ""),
        projection_ids,
        scanned_types,
        input);

    // Store the total number of endpoints in the bind data so progress
    // can be reported across all endpoints.
    bind_data.set_endpoint_count(result->total_endpoints());

    return result;
  }

  double AirportTakeFlightScanProgress(ClientContext &, const FunctionData *data, const GlobalTableFunctionState *global_state)
  {
    return data->Cast<AirportTakeFlightBindData>().total_progress();
  }

  static std::vector<uint8_t> base64_decode(const std::string &base64_input)
  {
    BIO *bio, *b64;
    int decodeLen = (int)base64_input.size() * 3 / 4;
    std::vector<uint8_t> buffer(decodeLen);

    bio = BIO_new_mem_buf(base64_input.data(), (int)base64_input.length());
    b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    bio = BIO_push(b64, bio);

    int len = BIO_read(bio, buffer.data(), (int)buffer.size());
    buffer.resize(len);
    BIO_free_all(bio);
    return buffer;
  }

  struct LocationDataContents
  {
    std::string format;
    std::string uri;

    MSGPACK_DEFINE_MAP(format, uri)
  };

  static bool
  AirportLocalStateProcessEndpoint(ClientContext &context,
                                   const TableFunctionInitInput &input,
                                   const AirportTakeFlightBindData &bind_data,
                                   AirportArrowScanGlobalState &global_state,
                                   AirportArrowScanLocalState &local_state,
                                   const flight::FlightEndpoint endpoint)
  {
    auto flight_client = AirportAPI::FlightClientForLocation(bind_data.server_location());

    auto &location = endpoint.locations.front();

    auto server_location = location.ToString();

    local_state.lines_read = 0;
    local_state.chunk_offset = 0;
    local_state.chunk = make_uniq<ArrowArrayWrapper>();
    local_state.Reset();

    if (location.scheme() == "data")
    {
      arrow::util::Uri uri;

      // Now show to we get the rest of the url.
      AIRPORT_ARROW_ASSERT_OK_LOCATION(
          uri.Parse(location.ToString()),
          server_location,
          "airport_take_flight: parsing data endpoint");

      const std::string path = uri.path();

      // Find the comma separating metadata from payload
      auto comma_pos = path.find(',');
      if (comma_pos == std::string::npos)
      {
        throw AirportFlightException(server_location, "Invaid data URI format in endpoint");
      }

      std::string media_type = path.substr(0, comma_pos); // application/msgpack;base64

      if (media_type != "application/msgpack;base64" &&
          media_type != "application/x-msgpack-duckdb-function-call;base64")
      {
        throw AirportFlightException(server_location, "Invalid media type in data URI, should be application/msgpack;base64 or application/x-msgpack-duckdb-function-call;base64");
      }

      std::string base64_payload = path.substr(comma_pos + 1); // base64 encoded data

      std::vector<uint8_t> decoded = base64_decode(base64_payload);

      if (media_type == "application/x-msgpack-duckdb-function-call;base64")
      {
        // Now deal with the msgpack stuff.
        AIRPORT_MSGPACK_UNPACK_CONTAINER(AirportDuckDBFunctionCall,
                                         function_call_data,
                                         decoded,
                                         (&bind_data),
                                         "File to parse msgpack encoded DuckDB function call");

        D_ASSERT(!function_call_data.function_name.empty());
        D_ASSERT(!function_call_data.data.empty());

        auto parsed_info = AirportParseFunctionCallDetails(
            function_call_data,
            context,
            bind_data);

        auto local_scan_data = std::make_shared<AirportLocalScanData>(
            parsed_info.argument_values,
            parsed_info.named_params,
            context,
            parsed_info.func,
            bind_data.return_types(),
            bind_data.return_names(),
            *global_state.init_input());

        local_state.set_reader(local_scan_data);
      }
      else
      {
        // Now deal with the msgpack stuff.
        AIRPORT_MSGPACK_UNPACK(LocationDataContents,
                               location_data,
                               decoded,
                               server_location,
                               "File to parse msgpack encoded data uri");

        if (location_data.format != "ipc-stream" &&
            location_data.format != "ipc-file" &&
            location_data.format != "parquet")
        {
          throw AirportFlightException(server_location, "Unhandled data format in data URI: " + location_data.format);
        }

        // So the location can be a URL of various types, file://, s3://, gcs://

        if (location_data.uri.empty())
        {
          throw AirportFlightException(server_location, "Empty uri in data URI");
        }

        if (location_data.format == "parquet")
        {
          auto &instance = DatabaseInstance::GetDatabase(context);
          auto &parquet_scan_entry = AirportGetTableFunction(instance, "parquet_scan");
          auto &parquet_scan = parquet_scan_entry.functions.functions[0];

          // So the problem here is that we need to pass the actual return_types and return_names
          // that will be set in the output, otherwise, the output mapping is incorrect.
          auto local_scan_data = std::make_shared<AirportLocalScanData>(
              location_data.uri,
              context,
              parquet_scan,
              bind_data.return_types(),
              bind_data.return_names(),
              *global_state.init_input());

          local_state.set_reader(local_scan_data);
        }
        else if (location_data.format == "ipc-stream" || location_data.format == "ipc-file")
        {
          std::string actual_path;
          AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto fs,
                                           arrow::fs::FileSystemFromUriOrPath(location_data.uri, &actual_path),
                                           server_location,
                                           "airport_take_flight: parsing data URI");

          AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto input_file, fs->OpenInputFile(actual_path),
                                           server_location,
                                           "airport_take_flight: opening data URI");

          if (location_data.format == "ipc-stream")
          {
            AIRPORT_ASSIGN_OR_RAISE_LOCATION(
                auto reader,
                arrow::ipc::RecordBatchStreamReader::Open(input_file),
                server_location,
                "airport_take_flight: opening data URI")

            if (!reader->schema()->Equals(bind_data.schema()))
            {
              throw AirportFlightException(server_location, "Schema of data at" + location_data.uri + " does not match expected schema.");
            }

            local_state.set_reader(std::move(reader));
          }
          else if (location_data.format == "ipc-file")
          {
            AIRPORT_ASSIGN_OR_RAISE_LOCATION(
                auto reader,
                arrow::ipc::RecordBatchFileReader::Open(input_file),
                server_location,
                "airport_take_flight: opening data URI")

            if (!reader->schema()->Equals(bind_data.schema()))
            {
              throw AirportFlightException(server_location, "Schema of data at" + location_data.uri + " does not match expected schema.");
            }

            local_state.set_reader(std::move(reader));
          }
        }
      }
    }

    else
    {
      if (location != flight::Location::ReuseConnection())
      {
        AIRPORT_ASSIGN_OR_RAISE_LOCATION(flight_client,
                                         flight::FlightClient::Connect(location),
                                         location.ToString(),
                                         "");
        server_location = bind_data.server_location();
      }

      const auto &descriptor = bind_data.descriptor();

      arrow::flight::FlightCallOptions call_options;
      airport_add_normal_headers(call_options,
                                 bind_data.take_flight_params(),
                                 bind_data.trace_id(),
                                 descriptor);

      if (bind_data.skip_producing_result_for_update_or_delete)
      {
        // This is a special case where the result of the scan should be skipped.
        // This is useful when the scan is being used to update or delete rows.
        // For a table that doesn't actually produce row ids, so filtering cannot be applied.
        call_options.headers.emplace_back("airport-skip-producing-results", "1");
      }

      AIRPORT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
          auto stream,
          flight_client->DoGet(
              call_options,
              endpoint.ticket),
          server_location,
          descriptor,
          "");

      // FIXME: make sure that the schema returned from the server is the same as
      // what we were expecting.

      // So the bind data won't have a stream set on it,
      // but the local state will, the prokblem is the CreateStream
      // callback doesn't have a reference to the local state.

      // Can we reuse the chunk?
      local_state.set_reader(std::move(stream));
    }

    if (!std::holds_alternative<std::shared_ptr<AirportLocalScanData>>(local_state.reader()))
    {
      local_state.set_stream(
          AirportProduceArrowScan(bind_data,
                                  input.column_ids,
                                  input.filters.get(),
                                  bind_data.get_progress_counter(0),
                                  // No need for the last metadata message.
                                  nullptr,
                                  bind_data.schema(),
                                  bind_data,
                                  local_state));
    }
    else
    {
      // If we're using a scan function, no need to create an arrow stream.
      local_state.set_stream(nullptr);
    }

    local_state.column_ids = input.column_ids;
    local_state.filters = (TableFilterSet *)input.filters.get();

    // Projection pushdown is always enabled.
    D_ASSERT(bind_data.projection_pushdown_enabled);
    if (!input.projection_ids.empty())
    {
      local_state.all_columns.Initialize(context, global_state.scanned_types());
    }
    if (!AirportArrowScanParallelStateNext(local_state,
                                           global_state,
                                           bind_data,
                                           context))
    {
      return false;
    }
    return true;
  }

  static unique_ptr<LocalTableFunctionState>
  AirportArrowScanInitLocalInternal(ClientContext &context, TableFunctionInitInput &input,
                                    GlobalTableFunctionState *global_state_p)
  {
    auto &bind_data = input.bind_data->Cast<AirportTakeFlightBindData>();
    auto &global_state = global_state_p->Cast<AirportArrowScanGlobalState>();

    auto &endpoint_opt = global_state.GetNextEndpoint();
    D_ASSERT(endpoint_opt != std::nullopt);

    auto current_chunk = make_uniq<ArrowArrayWrapper>();
    auto result = make_uniq<AirportArrowScanLocalState>(
        std::move(current_chunk),
        context,
        input);

    AirportLocalStateProcessEndpoint(context,
                                     input,
                                     bind_data,
                                     global_state,
                                     *result,
                                     *endpoint_opt);
    return result;
  }

  unique_ptr<LocalTableFunctionState> AirportArrowScanInitLocal(ExecutionContext &context,
                                                                TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state_p)
  {
    return AirportArrowScanInitLocalInternal(context.client, input, global_state_p);
  }

  static BindInfo AirportTakeFlightGetBindInfo(const optional_ptr<FunctionData> bind_data_p)
  {
    auto &bind_data = bind_data_p->Cast<AirportTakeFlightBindData>();
    // I know I'm dropping the const here, fix this later.
    AirportTableEntry *table_entry = (AirportTableEntry *)bind_data.table_entry();

    D_ASSERT(table_entry != nullptr);
    BindInfo bind_info(*table_entry);
    return bind_info;
  }

  void AirportAddTakeFlightFunction(ExtensionLoader &loader)
  {

    auto take_flight_function_set = TableFunctionSet("airport_take_flight");

    auto take_flight_function_with_descriptor = TableFunction(
        "airport_take_flight",
        {LogicalType::VARCHAR, LogicalType::ANY},
        AirportTakeFlight,
        take_flight_bind,
        AirportArrowScanInitGlobal,
        AirportArrowScanInitLocal);

    take_flight_function_with_descriptor.named_parameters["auth_token"] = LogicalType::VARCHAR;
    take_flight_function_with_descriptor.named_parameters["secret"] = LogicalType::VARCHAR;
    take_flight_function_with_descriptor.named_parameters["ticket"] = LogicalType::BLOB;
    take_flight_function_with_descriptor.named_parameters["headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
    take_flight_function_with_descriptor.named_parameters["at_unit"] = LogicalType::VARCHAR;
    take_flight_function_with_descriptor.named_parameters["at_value"] = LogicalType::ANY;

    take_flight_function_with_descriptor.pushdown_complex_filter = AirportTakeFlightComplexFilterPushdown;

    take_flight_function_with_descriptor.cardinality = AirportTakeFlightCardinality;
    //    take_flight_function_with_descriptor.get_batch_index = nullptr;
    take_flight_function_with_descriptor.projection_pushdown = true;
    take_flight_function_with_descriptor.filter_pushdown = false;
    take_flight_function_with_descriptor.table_scan_progress = AirportTakeFlightScanProgress;
    take_flight_function_set.AddFunction(take_flight_function_with_descriptor);

    auto take_flight_function_with_pointer = TableFunction(
        "airport_take_flight",
        {LogicalType::POINTER, LogicalType::POINTER, LogicalType::VARCHAR},
        AirportTakeFlight,
        take_flight_bind_with_pointer,
        AirportArrowScanInitGlobal,
        AirportArrowScanInitLocal);

    take_flight_function_with_pointer.named_parameters["auth_token"] = LogicalType::VARCHAR;
    take_flight_function_with_pointer.named_parameters["secret"] = LogicalType::VARCHAR;
    take_flight_function_with_pointer.named_parameters["at_unit"] = LogicalType::VARCHAR;
    take_flight_function_with_pointer.named_parameters["at_value"] = LogicalType::ANY;
    take_flight_function_with_pointer.pushdown_complex_filter = AirportTakeFlightComplexFilterPushdown;

    // Add support for optional named paraemters that would be appended to the descriptor
    // of the flight, ideally parameters would be JSON encoded.

    take_flight_function_with_pointer.cardinality = AirportTakeFlightCardinality;
    //    take_flight_function_with_pointer.get_batch_index = nullptr;
    take_flight_function_with_pointer.projection_pushdown = true;
    take_flight_function_with_pointer.filter_pushdown = false;
    take_flight_function_with_pointer.table_scan_progress = AirportTakeFlightScanProgress;
    take_flight_function_with_pointer.statistics = AirportTakeFlightStatistics;
    take_flight_function_with_pointer.get_bind_info = AirportTakeFlightGetBindInfo;

    take_flight_function_set.AddFunction(take_flight_function_with_pointer);

    loader.RegisterFunction(take_flight_function_set);
  }

  std::string AirportNameForField(const string &name, idx_t col_idx)
  {
    if (name.empty())
    {
      return string("v") + to_string(col_idx);
    }
    return name;
  }
}