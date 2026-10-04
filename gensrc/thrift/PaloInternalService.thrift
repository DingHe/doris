// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp doris
namespace java org.apache.doris.thrift

include "Status.thrift"
include "Types.thrift"
include "Exprs.thrift"
include "Descriptors.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "DataSinks.thrift"
include "Data.thrift"
include "RuntimeProfile.thrift"

// constants for TQueryOptions.num_nodes
const i32 NUM_NODES_ALL = 0
const i32 NUM_NODES_ALL_RACKS = -1

// constants for TPlanNodeId
const i32 INVALID_PLAN_NODE_ID = -1

// Constant default partition ID, must be < 0 to avoid collisions
const i64 DEFAULT_PARTITION_ID = -1;

enum TQueryType {
    SELECT = 0,
    LOAD = 1,
    EXTERNAL = 2
}

enum TErrorHubType {
    MYSQL = 0,
    BROKER = 1,
    NULL_TYPE = 2
}

enum TPrefetchMode {
    NONE = 0,
    HT_BUCKET = 1
}

struct TMysqlErrorHubInfo {
    1: required string host;
    2: required i32 port;
    3: required string user;
    4: required string passwd;
    5: required string db;
    6: required string table;
}

struct TBrokerErrorHubInfo {
    1: required Types.TNetworkAddress broker_addr;
    2: required string path;
    3: required map<string, string> prop;
}

struct TLoadErrorHubInfo {
    1: required TErrorHubType type = TErrorHubType.NULL_TYPE;
    2: optional TMysqlErrorHubInfo mysql_info;
    3: optional TBrokerErrorHubInfo broker_info;
}

struct TResourceLimit {
    1: optional i32 cpu_limit
}

enum TSerdeDialect {
  DORIS = 0,
  PRESTO = 1,
  HIVE = 2
}

// Query options that correspond to PaloService.PaloQueryOptions,
// with their respective defaults
struct TQueryOptions {
  1: optional bool abort_on_error = 0 // Deprecated
  2: optional i32 max_errors = 0
  3: optional bool disable_codegen = 1 // Deprecated
  4: optional i32 batch_size = 0
  5: optional i32 num_nodes = NUM_NODES_ALL
  6: optional i64 max_scan_range_length = 0 // Deprecated
  7: optional i32 max_scanners_concurrency = 0
  8: optional i32 max_io_buffers = 0 // Deprecated
  9: optional bool allow_unsupported_formats = 0 // Deprecated
  10: optional i64 default_order_by_limit = -1
  // 11: optional string debug_action = "" // Never used
  12: optional i64 mem_limit = 2147483648
  13: optional bool abort_on_default_limit_exceeded = 0 // Deprecated
  14: optional i32 query_timeout = 3600
  15: optional bool is_report_success = 0
  16: optional i32 codegen_level = 0 // Deprecated
  // INT64::MAX
  17: optional i64 kudu_latest_observed_ts = 9223372036854775807 // Deprecated
  18: optional TQueryType query_type = TQueryType.SELECT
  19: optional i64 min_reservation = 0 // Deprecated
  20: optional i64 max_reservation = 107374182400 // Deprecated
  21: optional i64 initial_reservation_total_claims = 2147483647 // TODO chenhao // Deprecated
  22: optional i64 buffer_pool_limit = 2147483648 // Deprecated

  // The default spillable buffer size in bytes, which may be overridden by the planner.
  // Defaults to 2MB.
  23: optional i64 default_spillable_buffer_size = 2097152; //Deprecated

  // The minimum spillable buffer to use. The planner will not choose a size smaller than
  // this. Defaults to 64KB.
  24: optional i64 min_spillable_buffer_size = 65536; //Deprecated

  // The maximum size of row that the query will reserve memory to process. Processing
  // rows larger than this may result in a query failure. Defaults to 512KB, e.g.
  // enough for a row with 15 32KB strings or many smaller columns.
  //
  // Different operators handle this option in different ways. E.g. some simply increase
  // the size of all their buffers to fit this row size, whereas others may use more
  // sophisticated strategies - e.g. reserving a small number of buffers large enough to
  // fit maximum-sized rows.
  25: optional i64 max_row_size = 524288; //Deprecated

  // stream preaggregation
  26: optional bool disable_stream_preaggregations = false;

  // multithreaded degree of intra-node parallelism
  27: optional i32 mt_dop = 0; // Deprecated
  // if this is a query option for LOAD, load_mem_limit should be set to limit the mem comsuption
  // of load channel.
  28: optional i64 load_mem_limit = 0;
  // see BE config `doris_max_scan_key_num` for details
  // if set, this will overwrite the BE config.
  29: optional i32 max_scan_key_num;
  // see BE config `max_pushdown_conditions_per_column` for details
  // if set, this will overwrite the BE config.
  30: optional i32 max_pushdown_conditions_per_column
  // whether enable spilling to disk
  // 31: optional bool enable_spilling = false;
  // whether enable parallel merge in exchange node
  32: optional bool enable_enable_exchange_node_parallel_merge = false; // deprecated

  // Time in ms to wait until runtime filters are delivered.
  33: optional i32 runtime_filter_wait_time_ms = 1000

  // if the right table is greater than this value in the hash join,  we will ignore IN filter
  34: optional i32 runtime_filter_max_in_num = 1024;

  // the resource limitation of this query
  42: optional TResourceLimit resource_limit

  // show bitmap data in result, if use this in mysql cli may make the terminal
  // output corrupted character
  43: optional bool return_object_data_as_binary = false

  // trim tailing spaces while querying external table and stream load
  44: optional bool trim_tailing_spaces_for_external_table_query = false

  45: optional bool enable_function_pushdown;

  46: optional string fragment_transmission_compression_codec;

  48: optional bool enable_local_exchange;

  // For debug purpose, dont' merge unique key and agg key when reading data.
  49: optional bool skip_storage_engine_merge = false

  // For debug purpose, skip delete predicates when reading data
  50: optional bool skip_delete_predicate = false

  51: optional bool enable_new_shuffle_hash_method

  52: optional i32 be_exec_version = 0

  53: optional i32 partitioned_hash_join_rows_threshold = 0 // deprecated

  54: optional bool enable_share_hash_table_for_broadcast_join

  55: optional bool check_overflow_for_decimal = true

  // For debug purpose, skip delete bitmap when reading data
  56: optional bool skip_delete_bitmap = false
  // non-pipelinex engine removed. always true.
  57: optional bool enable_pipeline_engine = true

  58: optional i32 repeat_max_num = 0 // Deprecated

  // deprecated, use spill_sort_mem_limit
  59: optional i64 external_sort_bytes_threshold = 0

  // deprecated
  60: optional i32 partitioned_hash_agg_rows_threshold = 0 // deprecated

  61: optional bool enable_file_cache = false

  62: optional i32 insert_timeout = 14400

  63: optional i32 execution_timeout = 3600

  64: optional bool dry_run_query = false

  65: optional bool enable_common_expr_pushdown = false; // deprecated

  66: optional i32 parallel_instance = 1
  // Indicate where useServerPrepStmts enabled
  67: optional bool mysql_row_binary_format = false;
  // Not used anymore
  68: optional i64 external_agg_bytes_threshold = 0 // deprecated

  // Not used anymore, use spill_aggregation_partition_count
  69: optional i32 external_agg_partition_bits = 4 // deprecated

  // Specify base path for file cache
  70: optional string file_cache_base_path

  71: optional bool enable_parquet_lazy_mat = true

  72: optional bool enable_orc_lazy_mat = true

  73: optional i64 scan_queue_mem_limit

  74: optional bool enable_scan_node_run_serial = false; // deprecated

  75: optional bool enable_insert_strict = false;

  76: optional bool enable_inverted_index_query = true;

  77: optional bool truncate_char_or_varchar_columns = false

  78: optional bool enable_hash_join_early_start_probe = false // deprecated
  // non-pipelinex engine removed. always true.
  79: optional bool enable_pipeline_x_engine = true;

  80: optional bool enable_memtable_on_sink_node = false;

  81: optional bool enable_delete_sub_predicate_v2 = false; // deprecated

  // A tag used to distinguish fe start epoch.
  82: optional i64 fe_process_uuid = 0;

  83: optional i32 inverted_index_conjunction_opt_threshold = 1000;
  // A seperate flag to indicate whether to enable profile, not
  // use is_report_success any more
  84: optional bool enable_profile = false;
  85: optional bool enable_page_cache = false;
  86: optional i32 analyze_timeout = 43200;

  87: optional bool faster_float_convert = false; // deprecated

  88: optional bool enable_decimal256 = false;

  89: optional bool enable_local_shuffle = false;
  // For emergency use, skip missing version when reading rowsets
  90: optional bool skip_missing_version = false;

  91: optional bool runtime_filter_wait_infinitely = false;

  92: optional i32 condition_cache_digest = 0;
  
  93: optional i32 inverted_index_max_expansions = 50;

  94: optional i32 inverted_index_skip_threshold = 50;

  95: optional bool enable_parallel_scan = false;

  96: optional i32 parallel_scan_max_scanners_count = 0;

  97: optional i64 parallel_scan_min_rows_per_scanner = 0;

  98: optional bool skip_bad_tablet = false;
  // Increase concurrency of scanners adaptively, the maxinum times to scale up
  99: optional double scanner_scale_up_ratio = 0; //deprecated

  100: optional bool enable_distinct_streaming_aggregation = true;

  // deprecated
  101: optional bool enable_join_spill = false

  // deprecated
  102: optional bool enable_sort_spill = false

  // deprecated
  103: optional bool enable_agg_spill = false

  104: optional i64 min_revocable_mem = 0

  105: optional i64 spill_streaming_agg_mem_limit = 0;

  // max rows of each sub-queue in DataQueue.
  106: optional i64 data_queue_max_blocks = 0;
  
  // deprecated
  107: optional bool enable_common_expr_pushdown_for_inverted_index = false;
  108: optional i64 local_exchange_free_blocks_limit;

  109: optional bool enable_force_spill = false;

  110: optional bool enable_parquet_filter_by_min_max = true
  111: optional bool enable_orc_filter_by_min_max = true

  112: optional i32 max_column_reader_num = 0

  113: optional bool enable_local_merge_sort = false; // deprecated

  114: optional bool enable_parallel_result_sink = false;

  115: optional bool enable_short_circuit_query_access_column_store = false;

  116: optional bool enable_no_need_read_data_opt = true;
  
  117: optional bool read_csv_empty_line_as_null = false;

  118: optional TSerdeDialect serde_dialect = TSerdeDialect.DORIS;

  119: optional bool enable_match_without_inverted_index = true;

  120: optional bool enable_fallback_on_missing_inverted_index = true;

  121: optional bool keep_carriage_return = false; // \n,\r\n split line in CSV.

  122: optional i32 runtime_bloom_filter_min_size = 1048576;

  //Access Parquet/ORC columns by name by default. Set this property to `false` to access columns
  //by their ordinal position in the Hive table definition.  
  123: optional bool hive_parquet_use_column_names = true;
  124: optional bool hive_orc_use_column_names = true;

  125: optional bool enable_segment_cache = true;

  126: optional i32 runtime_bloom_filter_max_size = 16777216;
  127: optional i32 in_list_value_count_threshold = 10;
  // We need this two fields to make sure thrift id on master is compatible with other branch.
  128: optional bool enable_verbose_profile = false;  // deprecated
  129: optional i32 rpc_verbose_profile_max_instance_count = 0;

  130: optional bool enable_adaptive_pipeline_task_serial_read_on_limit = true;
  131: optional i32 adaptive_pipeline_task_serial_read_on_limit = 10000;

  132: optional i32 parallel_prepare_threshold = 0;
  133: optional i32 partition_topn_max_partitions = 1024;
  134: optional i32 partition_topn_pre_partition_rows = 1000;

  135: optional bool enable_parallel_outfile = false;

  136: optional bool enable_phrase_query_sequential_opt = true; // deprecated
  
  137: optional bool enable_auto_create_when_overwrite = false;

  138: optional i64 orc_tiny_stripe_threshold_bytes = 8388608;
  139: optional i64 orc_once_max_read_bytes = 8388608;
  140: optional i64 orc_max_merge_distance_bytes = 1048576;

  141: optional bool ignore_runtime_filter_error = false;

  142: optional bool enable_fixed_len_to_uint32_v2 = false;
  143: optional bool enable_shared_exchange_sink_buffer = true;


  144: optional bool enable_inverted_index_searcher_cache = true;
  145: optional bool enable_inverted_index_query_cache = true;
  146: optional bool enable_condition_cache = false; //deprecated 

  147: optional i32 profile_level = 1;

  148: optional i32 min_scanners_concurrency = 1;
  149: optional i32 min_scan_scheduler_concurrency = 0; //deprecated
  // Controls runtime-filter partition pruning for readers that honor this option.
  // FileScannerV2 always enables safe partition pruning.
  150: optional bool enable_runtime_filter_partition_prune = true;

  // The minimum memory that an operator required to run.
  151: optional i32 minimum_operator_memory_required_kb = 1024;

  152: optional bool enable_mem_overcommit = true; // deprecated
  153: optional i32 query_slot_count = 0;
  154: optional bool enable_spill = false
  155: optional bool enable_reserve_memory = true
  156: optional i32 revocable_memory_high_watermark_percent = -1

  157: optional i64 spill_sort_mem_limit = 134217728
  158: optional i64 spill_sort_batch_bytes = 8388608
  159: optional i32 spill_aggregation_partition_count = 32
  160: optional i32 spill_hash_join_partition_count = 32
  161: optional i64 low_memory_mode_buffer_limit = 33554432
  162: optional bool dump_heap_profile_when_mem_limit_exceeded = false
  163: optional bool inverted_index_compatible_read = false
  164: optional bool check_orc_init_sargs_success = false
  165: optional i32 exchange_multi_blocks_byte_size = 262144
  // true to use strict cast mode.
  166: optional bool enable_strict_cast = false
  167: optional bool new_version_unix_timestamp = false

  168: optional i32 hnsw_ef_search = 32;
  169: optional bool hnsw_check_relative_distance = true;
  170: optional bool hnsw_bounded_queue = true;

  171: optional bool optimize_index_scan_parallelism = false;

  172: optional bool enable_prefer_cached_rowset
  173: optional i64 query_freshness_tolerance_ms
  174: optional i64 merge_read_slice_size = 8388608;

  175: optional bool enable_fuzzy_blockable_task = false;
  176: optional list<i32> shuffled_agg_ids;

  177: optional bool enable_extended_regex = false;
  // Target file size in bytes for Iceberg write operations
  // Default 0 means use config::iceberg_sink_max_file_size
  178: optional i64 iceberg_write_target_file_size_bytes = 0;
  179: optional bool enable_parquet_filter_by_bloom_filter = true;
  180: optional i32 max_file_scanners_concurrency = 0;
  181: optional i32 min_file_scanners_concurrency = 0;
  182: optional i32 ivf_nprobe = 32;
  // Enable hybrid sorting: dynamically selects between PdqSort and TimSort based on 
  // runtime profiling to choose the most efficient algorithm for the data pattern
  183: optional bool enable_use_hybrid_sort = false;
  184: optional i32 cte_max_recursion_depth;

  185: optional bool enable_parquet_file_page_cache = true;

  186: optional bool enable_streaming_agg_hash_join_force_passthrough;

  187: optional bool enable_distinct_streaming_agg_force_passthrough;

  188: optional bool enable_broadcast_join_force_passthrough;

  189: optional bool enable_aggregate_function_null_v2 = false;

  195: optional bool enable_left_semi_direct_return_opt;

  200: optional bool enable_adjust_conjunct_order_by_cost;
  // Use paimon-cpp to read Paimon splits on BE
  201: optional bool enable_paimon_cpp_reader = false;

  // Whether all fragments of this query are assigned to a single backend.
  // When true, the streaming aggregation operator can use more aggressive
  // hash table expansion thresholds since all data is local.
  202: optional bool single_backend_query = false;

  203: optional bool enable_inverted_index_wand_query = true;

  // Per-read/per-write buffer size used during spill I/O, in bytes. Controls the
  // I/O batch size for spill write and merge read. This value can be overridden
  // per-query by setting the session variable `spill_buffer_size_bytes` in FE.
  // Default is 8MB.
  204: optional i64 spill_buffer_size_bytes = 8388608

  // Per-sink memory limit after spill is triggered. When a sink operator's revocable
  // memory exceeds the corresponding threshold, it proactively spills to disk.
  // Default is 64MB for all three.
  205: optional i64 spill_join_build_sink_mem_limit_bytes = 67108864
  206: optional i64 spill_aggregation_sink_mem_limit_bytes = 67108864
  207: optional i64 spill_sort_sink_mem_limit_bytes = 67108864

  // Total memory budget for the sort merge phase after spill. Divided by
  // spill_buffer_size_bytes gives the number of files merged in parallel.
  // Default is 64MB.
  208: optional i64 spill_sort_merge_mem_limit_bytes = 67108864

  // Maximum depth for repartitioning recursion. Controls how many recursive
  // repartition rounds are allowed before giving up and treating a partition
  // as terminal. This value can be overridden per-query by setting the
  // session variable `spill_repartition_max_depth` in FE. Default is 8.
  209: optional i32 spill_repartition_max_depth = 8

  210: optional double max_scan_mem_ratio = 0.3;
  211: optional bool enable_adaptive_scan = false;

  212: optional bool enable_local_exchange_before_agg = true;

  213: optional i64 file_presigned_url_ttl_seconds = 3600;
  214: optional i32 embed_max_batch_size = 5;
  215: optional i64 ai_context_window_size = 131072;

  // Use Rust-based Lance reader for FORMAT_LANCE scan ranges
  216: optional bool enable_rust_lance_reader = false; // deprecated
  217: optional bool new_version_percentile = false

  // Adaptive batch size: target output block size in bytes. Valid range [1MB, 512MB].
  // Default 8MB. Sent by FE session variable preferred_block_size_bytes.
  218: optional i64 preferred_block_size_bytes = 8388608

  // Push LIMIT into SegmentIterator when safe.
  219: optional bool enable_segment_limit_pushdown = true

  220: optional bool enable_ann_index_result_cache = true
  // ANN search falls back to exact vector distance evaluation when candidate rows
  // before ANN search are less than this value. 0 disables the absolute threshold.
  221: optional i64 ann_index_candidate_rows_threshold = 0
  // Candidate row ratio threshold against segment rows. Existing default is 0.3.
  222: optional double ann_index_candidate_rows_percent_threshold = 0.3

  // enable plan local exchange node in fe
  223: optional bool enable_local_shuffle_planner;

  // Controls expression-based ZoneMap pruning for readers that honor this option.
  // FileScannerV2 always enables safe expression ZoneMap pruning.
  224: optional bool enable_expr_zonemap_filter = true

  225: optional i64 runtime_filter_tree_publish_max_send_bytes = 268435456

  226: optional bool enable_prune_nested_column = false;
  227: optional bool new_version_bitmap_op_count = false;
  228: optional bool enable_local_exchange_before_streaming_agg = false;
  // FE is the receiver of fragment reports, so BE must also honor its message limit.
  229: optional i32 coordinator_thrift_max_message_size;
  // FE can explicitly and idempotently acknowledge external-file commit reports.
  230: optional bool supports_external_file_report_ack = false;
  // For cloud, to control if the content would be written into file cache
  // In write path, to control if the content would be written into file cache.
  // In read path, read from file cache or remote storage when execute query.
  1000: optional bool disable_file_cache = false
  1001: optional i32 file_cache_query_limit_percent = -1
  1002: optional bool enable_file_scanner_v2 = false
  1003: optional bool enable_topn_lazy_mat_phase2_no_write_file_cache = false
  1004: optional i64 file_cache_query_limit_bytes = -1
}


// A scan range plus the parameters needed to execute that scan.
struct TScanRangeParams {
  1: required PlanNodes.TScanRange scan_range
  2: optional i32 volume_id = -1
}

struct TRuntimeFilterTargetParamsV2 {
  1: required list<Types.TUniqueId> target_fragment_instance_ids
  // The address of the instance where the fragment is expected to run
  2: required Types.TNetworkAddress target_fragment_instance_addr
  3: optional list<i32> target_fragment_ids
}

struct TRuntimeFilterParams {
  // Runtime filter merge instance address. Used if this filter has a remote target
  1: optional Types.TNetworkAddress runtime_filter_merge_addr

  // Runtime filter ID to the runtime filter desc
  // Used if this filter has a remote target
  3: optional map<i32, PlanNodes.TRuntimeFilterDesc> rid_to_runtime_filter

  // Number of Runtime filter producers
  // Used if this filter has a remote target
  4: optional map<i32, i32> runtime_filter_builder_num

  // Used if this filter has a remote target
  5: optional map<i32, list<TRuntimeFilterTargetParamsV2>> rid_to_target_paramv2
}

// Parameters for a single execution instance of a particular TPlanFragment
// TODO: for range partitioning, we also need to specify the range boundaries
struct TPlanFragmentExecParams {
  // a globally unique id assigned to the entire query
  1: required Types.TUniqueId query_id

  // a globally unique id assigned to this particular execution instance of
  // a TPlanFragment
  2: required Types.TUniqueId fragment_instance_id
  // all fields before 14 are deleted
  // 14: optional list<i32> topn_filter_source_node_ids //deprecated
}

// Global query parameters assigned by the coordinator.
struct TQueryGlobals {
  // String containing a timestamp set as the current time.
  // Format is yyyy-MM-dd HH:mm:ss
  1: required string now_string

  // To support timezone in Doris. timestamp_ms is the millisecond uinix timestamp for
  // this query to calculate time zone relative function
  2: optional i64 timestamp_ms

  // time_zone is the timezone this query used.
  // If this value is set, BE will ignore now_string
  3: optional string time_zone

  // Set to true if in a load plan, the max_filter_ratio is 0.0
  4: optional bool load_zero_tolerance = false

  5: optional i32 nano_seconds

  // Locale name used for month/day names formatting, e.g. en_US
  6: optional string lc_time_names
}


// Service Protocol Details

enum PaloInternalServiceVersion {
  V1 = 0
}

struct TTxnParams {
  1: optional bool need_txn
  2: optional string token
  3: optional i64 thrift_rpc_timeout_ms
  4: optional string db
  5: optional string tbl
  6: optional string user_ip
  7: optional i64 txn_id
  8: optional Types.TUniqueId fragment_instance_id
  9: optional i64 db_id
  10: optional double max_filter_ratio
  // For load task with transaction, use this to indicate we use pipeline or not
  // non-pipelinex engine removed. always true.
  11: optional bool enable_pipeline_txn_load = true;
}

// Definition of global dict, global dict is used to accelerate query performance of low cardinality data
struct TColumnDict {
  1: optional Types.TPrimitiveType type
  2: list<string> str_dict  // map one string to a integer, using offset as id
}

struct TGlobalDict {
  1: optional map<i32, TColumnDict> dicts,  // map dict_id to column dict
  2: optional map<i32, i32> slot_dicts // map from slot id to column dict id, because 2 or more column may share the dict
}

struct TPipelineWorkloadGroup {
  1: optional i64 id
  2: optional string name
  3: optional map<string, string> properties
  4: optional i64 version
}

struct TFoldConstantParams {
  1: required map<string, map<string, Exprs.TExpr>> expr_map
  2: required TQueryGlobals query_globals
  3: optional bool vec_exec
  4: optional TQueryOptions query_options
  5: optional Types.TUniqueId query_id
  6: optional bool is_nereids
}

struct TTabletWithPartition {
    1: required i64 partition_id
    2: required i64 tablet_id
    3: optional i64 binlog_tablet_id
}

struct TFetchDataResult {
    // result batch
    1: required Data.TResultBatch result_batch
    // end of stream flag
    2: required bool eos
    // packet num used check lost of packet
    3: required i32 packet_num
    // Operation result
    4: optional Status.TStatus status
}

// For cloud
enum TCompoundType {
    UNKNOWN = 0,
    AND = 1,
    OR = 2,
    NOT = 3,
}

struct TAIResource {
  1: required string endpoint
  2: required string provider_type
  3: required string model_name
  4: optional string api_key
  5: optional double temperature
  6: optional i64 max_tokens
  7: optional i32 max_retries
  8: optional i32 retry_delay_second
  9: optional string anthropic_version
  10: optional i32 dimensions
}

struct TCondition {
    1:  required string column_name
    2:  required string condition_op
    3:  required list<string> condition_values
    // In delete condition, the different column may have same column name, need
    // using unique id to distinguish them
    4:  optional i32 column_unique_id
    5:  optional bool marked_by_runtime_filter = false // deprecated

    // For cloud
    1000: optional TCompoundType compound_type = TCompoundType.UNKNOWN
}

struct TPipelineInstanceParams {
  1: required Types.TUniqueId fragment_instance_id
  // deprecated
  2: optional bool build_hash_table_for_broadcast_join = false;
  3: required map<Types.TPlanNodeId, list<TScanRangeParams>> per_node_scan_ranges
  4: optional i32 sender_id
  5: optional TRuntimeFilterParams runtime_filter_params
  6: optional i32 backend_num
  7: optional map<Types.TPlanNodeId, bool> per_node_shared_scans // deprecated
  8: optional list<i32> topn_filter_source_node_ids // deprecated after we set topn_filter_descs
  9: optional list<PlanNodes.TTopnFilterDesc> topn_filter_descs
}
// 在 FE（Frontend）生成物理查询计划后，它会通过 bRPC 将这个结构体序列化传输给 BE（Backend）。
// BE 收到后，正是凭借该结构体中的元数据与具体指令，在 Pipeline 执行引擎（FragmentMgr / PipelineFragmentContext）中构建并调度运行真实的流水线任务。
// 分布式计划下发的元数据载体：它是 FE 向 BE 发送物理 Pipeline 片段的“总指挥图纸”，包含要执行的算子树（Plan Fragment）、数据源（Scan Range）、数据去向（Data Sink Dest Formation）等。
// 多 Pipeline Instance 并行构建凭证：在 Doris 新一代 Pipeline 执行引擎中，一个 Fragment 会根据并行度（Parallelism）拆分为多个 PipelineInstance。该结构体包含全局配置以及一个 local_params 列表（每个 Instance 的私有参数）。
// 资源、上下文与控制信令集锦：封装了查询的 Memory/Resource Group 限额、事务控制（Txn Conf）、动态过滤器信息（Runtime Filter/TopN Filter）、Group Commit 机制以及云原生/MOW（Merge-On-Write）特定标记。
// ExecPlanFragment
struct TPipelineFragmentParams {
  //服务内部协议版本。
  1: required PaloInternalServiceVersion protocol_version
  // 查询全局唯一标识（128 位 UUID）。
  2: required Types.TUniqueId query_id
  // 当前 Fragment 的 ID。
  // 在整棵查询树（Execution Tree）中唯一标识一个物理片段（如 HashJoin 节点所在的 Fragment）。
  3: optional i32 fragment_id
  // ** Exchange 接收节点的上游 Sender 数量映射表**。
  // Key 为 PlanNodeId（ExchangeNode），Value 为上游有多少个发送端 BE/Instance。ExchangeNode 靠此计数值判断何时接收完所有上游数据。
  4: required map<Types.TPlanNodeId, i32> per_exch_num_senders
  //** Descriptor 描述符表**。
  // 包含该 Fragment 运行所需的 TupleDescriptor、SlotDescriptor 和 TableDescriptor，定义了所有 Slot/Column 的内存数据类型、Nullability 及布局。
  5: optional Descriptors.TDescriptorTable desc_tbl
  // Deprecated
  // 早期用于 Yarn/CGroup 资源隔离，已被 Workload Group（ID 26）替代。
  6: optional Types.TResourceInfo resource_info
  // 数据下发的目标 BE/Instance 列表。
  // 定义当前 Fragment 处理完数据后，将数据发送（Data Stream Sink / Exchange Sender）至下游哪些 BE 节点的哪个 Exchange Node。
  7: list<DataSinks.TPlanFragmentDestination> destinations
  // 当前 Fragment 的 Sender 总数。
  // 标识当前物理片段共有多少个并发发送端 Instance。
  8: optional i32 num_senders
  // 是否随每个 Data Batch 附带 Query 统计信息。
  // 用于实时上报当前 Fragment 扫描的行数、CPU 耗时等 Profile 统计数据。
  9: optional bool send_query_statistics_with_every_batch
  // ** Coordinator（协调者节点，通常为 FE）的网络地址**。
  // 指示当前 BE 节点在查询结束或报错时，应向哪个 IP:Port 上报 Runtime Profile 和最终执行状态。
  10: optional Types.TNetworkAddress coord
  // 查询全局变量。
  // 包含当前 SQL 执行的系统时间（now()）、时区（Timezone）、Session ID 等所有算子共享的静态全局元数据。
  11: optional TQueryGlobals query_globals
  // 查询控制选项。
  // 封装了 SQL Session 级别的控制参数（如 exec_mem_limit、parallel_pipeline_task_num、query_timeout、enable_vectorized_engine 等）。
  12: optional TQueryOptions query_options
  // load job related
  // 导入任务 Label（仅导入/Stream Load 场景）。
  // 标识当前 Load Job 的用户唯一 Label，用于事务状态跟踪与幂等性保证。
  13: optional string import_label
  // 数据库名称。
  // 当前查询或导入操作目标表所属的 Database 名称。
  14: optional string db_name
  // 导入任务 Job ID。
  // FE 分配给当前 Broker Load / Routine Load / Stream Load 的唯一长整型 ID。
  15: optional i64 load_job_id
  // 导入错误 Hub 收集信息。
  // 当导入出现错误行（Error Rows）时，指定将错误数据写入外部存储（如 HDFS/S3/MySQL）配置。
  16: optional TLoadErrorHubInfo load_error_hub_info
  // 当前 BE 宿主机上运行的 Fragment 数量。
  // 用于调度器估算当前节点的负载压力与并发度。
  17: optional i32 fragment_num_on_host
  // 当前 BE 节点的唯一 Backend ID。
  // FE 节点元数据中记录的该 BE 的全局长整型编号。
  18: optional i64 backend_id
  // 是否需要等待两阶段执行触发信号。
  // 默认 false。若为 true，BE 在 Prepare 完毕后会挂起等待 FE 下发 Start 信号才开始实际计算，用于解决大型分布式 Join 的死锁问题。
  19: optional bool need_wait_execution_trigger = false
  // 共享 Hash Table 的 Instance ID 列表。
  // 在管道并行引擎中，同节点上的多个 Instance 可以共享同一个 Broadcast Hash Join 的 Build 侧哈希表以大幅节省内存。
  20: optional list<Types.TUniqueId> instances_sharing_hash_table
  // 精简参数裁剪标记。
  // 默认 false。标识当前 Thrift 结构是否经过裁剪瘦身，用于减少分布式网络传输开销。
  21: optional bool is_simplified_param = false;
  // 全局字典元数据。
  // 用于低基数字符串列（Low-Cardinality String）的全局字典优化。Scan Node 可以直接读取整数编码而非字符串，提升计算与传输性能。
  22: optional TGlobalDict global_dict  // scan node could use the global dict to encode the string value to an integer
  // 物理执行片段（算子树）。
  // 包含当前 Fragment 内所有的 TPlanNode（如 HashJoinNode、AggregateNode、OlapScanNode），构成真实的算子执行逻辑。 
  23: optional Planner.TPlanFragment fragment
  // 各 Pipeline Instance 的私有参数列表。
  // 每一个元素对应一个 PipelineInstance，里面包含了该 Instance 专属的 instance_id、扫描的具体 scan_ranges 等。
  24: list<TPipelineInstanceParams> local_params
  // ** Workload Group（资源组）配置**。
  // 指定该 Fragment 绑定的资源组，用于 BE 侧硬/软限制 CPU 线程配额、内存上限及查询排队。
  26: optional list<TPipelineWorkloadGroup> workload_groups
  // 事务配置参数。
  // 在 Stream Load 或两阶段提交写数据时，封装关联的 Transaction ID、Commit 状态及锁元数据。
  27: optional TTxnParams txn_conf
  // 目标表名称。
  // 当前写入或查询的主表表名。
  28: optional string table_name
  // scan node id -> scan range params, only for external file scan
  // 外表文件扫描参数映射。
  // 专用于外表（Hive/Iceberg/Hudi/S3/HDFS）。Key 为 ScanNodeId，Value 包含文件 Schema、压缩格式、列映射等。
  29: optional map<Types.TPlanNodeId, PlanNodes.TFileScanRangeParams> file_scan_params
  // 是否开启 Group Commit（组提交）。
  // 默认 false。标识当前导入任务是否走 Group Commit 管道以提高高频小批量导入的吞吐量。
  30: optional bool group_commit = false;
  // 单节点 Load Stream 连接管道数。
  // 在 Stream Load/数据导入 Pipeline 中，指定向下游每个 BE 建立的数据传输流通道数量。
  31: optional i32 load_stream_per_node // num load stream for each sink backend
  // 下游可见的 Load Stream 总管道数。
  // 下游接收端节点根据此总数来初始化 Data Stream Receiver 的配额。
  32: optional i32 total_load_streams // total num of load streams the downstream backend will see
  // 本地 Sink（Local Data Sink）数量。
  // 标识在当前 BE 节点内部通过进程内内存队列传输数据的 Sink 实例个数（规避网络序列化）。
  33: optional i32 num_local_sink
  // 分桶（Bucket）总数。
  // 针对 Bucket Shuffle Join 或分桶表扫描，记录当前表或者数据分布的 Hash 分桶总个数。
  34: optional i32 num_buckets
  // ** Bucket 序号到 Instance 索引的映射表**。
  // Key 为分桶序号（Bucket Seq），Value 为负责处理该分桶的 local_params 中的 Instance 索引。
  35: optional map<i32, i32> bucket_seq_to_instance_idx
  // 早期用于多 Instance 共享 Scan 句柄，现已重构成 Pipeline 引擎内部的统一异步 Scan 调度。
  36: optional map<Types.TPlanNodeId, bool> per_node_shared_scans // deprecated
  // 当前 BE 节点上的并发 Instance 数量。
  // 即该 Fragment 在当前 BE 上拆分出的 Pipeline 实例并行度（通常等于 parallel_pipeline_task_num）。
  37: optional i32 parallel_instances
  // 全集群当前 Fragment 的 Instance 总数。
  // 记录所有 BE 节点上该 Fragment 的并发 Instance 累加总和。
  38: optional i32 total_instances
  // ** Shuffle 索引到 Instance 索引的映射表**。
  // 用于 Bucket/Hash Shuffle 传输时，将数据 Hash 值精准路由到目标 Instance。
  39: optional map<i32, i32> shuffle_idx_to_instance_idx
  // 是否由 Nereids 新优化器生成。
  // 默认 true。标识该执行计划是否来源于 Doris 新一代现代架构优化器（Nereids），用于 BE 区分某些语法/类型行为。
  40: optional bool is_nereids = true;
  // WAL（Write-Ahead Log）日志 ID。
  // 在 Group Commit 或短事务写入开启时，对应存储引擎写入预写日志的唯一 WAL ID。
  41: optional i64 wal_id
  // 数据内容总长度。
  // 导入任务或数据传输时预估/确切的数据字节大小，用于内存预配。
  42: optional i64 content_length
  // 客户端直接连接的 FE 地址。
  // 记录用户发起 SQL 的原始 Master/Observer FE 网络地址，用于日志追溯与 Callback 上报。
  43: optional Types.TNetworkAddress current_connect_fe
  // Used by 2.1
  // 针对 Runtime TopN 动态过滤优化，指定产生 TopN 过滤条件的 Sort/Node ID。
  44: optional list<i32> topn_filter_source_node_ids
  // ** AI/LLM 扩展资源字典**。
  // 用于 Doris 集成内置 AI/向量函数/模型推理服务时的 AI 资源连接句柄。
  45: optional map<string, TAIResource> ai_resources
  // 是否需要在关闭时通知（用于递归 CTE）。
  46: optional bool need_notify_close

  // For cloud
  // （云原生/存储层）是否为 Unique Key MOW 表。
  // 标识目标表是否为 Merge-On-Write（写时合并）主键模型表，影响 Delete Bitmap 的生成策略。
  1000: optional bool is_mow_table;
  // （云原生/存储层）是否开启 TSO 检查。
  // 标识存算分离模式下是否使能 Timestamp Oracle 事务时间戳检查。
  1001: optional bool enable_tso;
}

// pull up runtime filter info from instance level to BE level
struct TRuntimeFilterInfo {
  // for join runtime filter and setop runtime filter
  1: optional TRuntimeFilterParams runtime_filter_params
  // for topn runtime filter
  2: optional list<PlanNodes.TTopnFilterDesc> topn_filter_descs
}
// 多 Fragment 批量打包传输（RPC 优化）：
// 在一个复杂的 SQL 查询中，FE 可能会将属于同一个 Query 的多个物理 Fragment 同时调度给同一个 BE 节点执行。如果为每个 Fragment 都发送一次单独的 RPC，会导致大量的网络开销。TPipelineFragmentParamsList 允许 FE 将这些 Fragment 打包在 params_list 中一次性 RPC 发送给 BE。
// 跨 Fragment 公共元数据解耦与复用（网络带宽与内存优化）：
// 在单 Fragment 参数 TPipelineFragmentParams 中，包含了大量的全局元数据（如描述符表 desc_tbl、文件扫描参数 file_scan_params、查询选项 query_options、Runtime Filter 路由信息等）。如果每个 Fragment 都带有一份完整的元数据，序列化体积会极大膨胀。
// TPipelineFragmentParamsList 将整个 Query 在当前 BE 节点共享的元数据提取到外层顶层。当 BE 接收并处理请求时（例如在前述 exec_plan_fragment 方法传入的 parent 参数），会直接从该外层结构体中共享这些全局信息，从而大幅降低 bRPC 的网络传输带宽与内存开销。
struct TPipelineFragmentParamsList {
  // Pipeline Fragment 参数列表。
  // 包含当前 BE 节点需要构建并执行的所有 TPipelineFragmentParams 结构体列表。每个元素代表一个具体的物理 Fragment 实例。
  1: optional list<TPipelineFragmentParams> params_list;
  // 共享描述符表（Descriptor Table）。
  // 整个 Query 在当前 BE 上共享的元数据描述符表，包含了所有 SlotDescriptor、TupleDescriptor 和 TableDescriptor。内层 Fragment 可直接共享此表，避免重复传输。
  2: optional Descriptors.TDescriptorTable desc_tbl;
  // scan node id -> scan range params, only for external file scan
  3: optional map<Types.TPlanNodeId, PlanNodes.TFileScanRangeParams> file_scan_params;
  4: optional Types.TNetworkAddress coord;
  5: optional TQueryGlobals query_globals;
  6: optional Types.TResourceInfo resource_info;
  // The total number of fragments on same BE host
  7: optional i32 fragment_num_on_host
  8: optional TQueryOptions query_options
  9: optional bool is_nereids = true;
  10: optional list<TPipelineWorkloadGroup> workload_groups
  11: optional Types.TUniqueId query_id
  12: optional list<i32> topn_filter_source_node_ids
  13: optional Types.TNetworkAddress runtime_filter_merge_addr
  14: optional TRuntimeFilterInfo runtime_filter_info
}
