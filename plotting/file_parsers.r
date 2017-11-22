
ConfigJson <- setRefClass("ConfigJson",
  fields = c('filepath', 'data_'),
  methods = list(
    initialize = function(para='null')
    {
        filepath <<- para
        data_ <<- read_json(filepath)
    },
    load = function()
    {
        return(data_)
    },
    get_exp_type = function()
    {
        json_data = load()
        if (!is.null(json_data[['exp_parameters']][['rw']])) {
            return("fiobench")
        } else if (!is.null(json_data[['exp_parameters']][['zone_size']])) {
            return("patternbench")
        } else {
            return("unknown")
        }
    },
    get_pattern_name = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['patternclass']])
    },
    get_over_provisioning = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['over_provisioning']])
    },
    get_ext4datamode = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['ext4datamode']])
    },
    get_ext4hasjournal = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['ext4hasjournal']])
    },
    get_user_traffic_size = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['traffic_size']])
    },
    get_stripe_size = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['stripe_size']])
    },
    get_cache_mapped_data_bytes = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['cache_mapped_data_bytes']])
    },
    get_page_size = function()
    {
        json_data = load()
        return(json_data[['flash_config']][['page_size']])
    },
    get_block_size = function()
    {
        json_data = load()
        return(json_data[['flash_config']][['page_size']] * 
               json_data[['flash_config']][['n_pages_per_block']])
    },
    get_ncq_depth = function()
    {
        json_data = load()
        return(json_data[['SSDFramework']][['ncq_depth']])
    },
    get_chunk_size = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['chunk_size']])
    },
    get_preallocate = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['preallocate']])
    },
    get_workload_class = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['workload_class']])
    },
    get_fio_rw = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['rw']])
    },
    get_fio_bs = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['bs']])
    },
    get_fio_norandommap = function()
    {
        json_data = load()
        return(null_is_zero(json_data[['exp_parameters']][['norandommap']]))
    },
    get_fio_fallocate = function()
    {
        json_data = load()
        return(null_is_zero(json_data[['exp_parameters']][['fallocate']]))
    },
    get_ext4hasjournal = function()
    {
        json_data = load()
        return(null_is_zero(json_data[['exp_parameters']][['ext4hasjournal']]))
    },
    get_has_hole = function()
    {
        json_data = load()
        return(null_is_zero(json_data[['exp_parameters']][['has_hole']]))
    },
    get_keep_size = function()
    {
        json_data = load()
        return(null_is_zero(json_data[['exp_parameters']][['keep_size']]))
    },
    get_filesystem = function()
    {
        json_data = load()
        return(json_data[['filesystem']])
    },
    get_exp_parameters_filesystem = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['filesystem']])
    },
    get_bench_to_run = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['bench_to_run']])
    },
    get_benchmarks = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['benchmarks']])
    },
    get_max_zeroout_kb = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['max_zeroout_kb']])
    },
    get_leveldb_write_buf_size = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['FLAGS_write_buffer_size']])
    },
    get_leveldb_n_instances = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['n_instances']])
    },
    get_leveldb_num = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['num']])
    },
    get_nkftl_n = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['nkftl_n']])
    },
    get_nkftl_k = function()
    {
        json_data = load()
        return(json_data[['exp_parameters']][['nkftl_k']])
    },
    get_device_path = function()
    {
        json_data = load()
        return(json_data[['device_path']])
    },
    get_expname = function()
    {
        json_data = load()
        return(json_data[['expname']])
    },
    get_contractbench_traffic_size = function()
    {
        json_data = load()
        size = json_data[['exp_parameters']][['bench']][['conf']][['traffic_size']]
        return(size)
    },
    get_contractbench_chunk_size = function()
    {
        json_data = load()
        size = json_data[['exp_parameters']][['bench']][['conf']][['chunk_size']]
        return(size)
    },
    get_contractbench_operation = function()
    {
        json_data = load()
        size = json_data[['exp_parameters']][['bench']][['conf']][['op']]
        return(size)
    },
    get_contractbench_space_size = function()
    {
        json_data = load()
        return( json_data[['exp_parameters']][['bench']][['conf']][['space_size']] )
    },
    get_contractbench_grouping = function()
    {
        json_data = load()
        return( json_data[['exp_parameters']][['bench']][['conf']][['grouping']] )
    },
    get_contractbench_is_aligned = function()
    {
        json_data = load()
        return( json_data[['exp_parameters']][['bench']][['conf']][['aligned']] )
    },
    get_ftl_type = function()
    {
        json_data = load()
        return( json_data[['ftl_type']] )
    },
    get_page_read_time = function()
    {
        json_data = load()
        return( json_data[['flash_config']][['t_R']] )
    },
    get_page_prog_time = function()
    {
        json_data = load()
        return( json_data[['flash_config']][['t_PROG']] )
    },
    get_block_erase_time = function()
    {
        json_data = load()
        return( json_data[['flash_config']][['t_BERS']] )
    },
    get_n_channels_per_dev = function()
    {
        json_data = load()
        return( json_data[['flash_config']][['n_channels_per_dev']] )
    },
    get_segment_bytes = function()
    {
        json_data = load()
        return( json_data[['segment_bytes']] )
    },
    get_filesnake_file_size = function()
    {
        json_data = load()
        return( json_data[['exp_parameters']][['benchconfs']][['file_size']] )
    },
    get_filesnake_write_pre_file = function()
    {
        json_data = load()
        return( json_data[['exp_parameters']][['benchconfs']][['write_pre_file']] )
    },
    get_sqlite_pattern = function()
    {
        json_data = load()
        return( json_data[['exp_parameters']][['benchconfs']][['pattern']] )
    },
    get_leveldb_benchconf_num = function()
    {
        json_data = load()
        return( json_data[['exp_parameters']][['benchconfs']][['num']] )
    },
    get_leveldb_max_key = function()
    {
        json_data = load()
        return( json_data[['exp_parameters']][['benchconfs']][['max_key']] )
    },
    get_snapshot_interval = function()
    {
        json_data = load()
        return( json_data[['snapshot_valid_ratios_interval']] )
    },
    get_lbabytes = function()
    {
        json_data = load()
        return( json_data[['exp_parameters']][['lbabytes']] )
    },
    get_testname = function()
    {
        json_data = load()
        return( json_data[['exp_parameters']][['testname']] )
    },
    get_testname_components = function()
    {
        testname = get_testname()
        items = unlist(strsplit(testname, '_'))
        names(items) = c('appname', 'rulename', 'rw', 'pattern')
        return(items)
    },
    get_testname_appname = function()
    {
        components = get_testname_components()
        return(components[['appname']])
    },
    get_testname_rulename = function()
    {
        components = get_testname_components()
        return(components[['rulename']])
    },
    get_testname_rw = function()
    {
        components = get_testname_components()
        return(components[['rw']])
    },
    get_testname_pattern = function()
    {
        components = get_testname_components()
        return(components[['pattern']])
    },
    get_mapping_cache_bytes = function()
    {
        json_data = load()
        return( json_data[['mapping_cache_bytes']] )
    },
    get_cache_mapped_data_bytes = function()
    {
        json_data = load()
        return( json_data[['exp_parameters']][['cache_mapped_data_bytes']] )
    },
    get_access_distribution = function()
    {
        json_data = load()
        return( json_data[['exp_parameters']][['access_distribution']] )
    },
    get_subexpname = function()
    {
        json_data = load()
        return( json_data[['subexpname']] )
    },
    get_appmix_appnames = function()
    {
        json_data = load()
        confs = json_data[['exp_parameters']][['appconfs']] 
        appnames = paste(confs[['name']], collapse='+')
        return(appnames)
    },
    get_appmix_first_appname = function()
    {
        json_data = load()
        confs = json_data[['exp_parameters']][['appconfs']] 
        appname = confs[['name']][[1]]
        return(appname)
    },
    get_appmix_n_insts = function()
    {
        json_data = load()
        confs = json_data[['exp_parameters']][['appconfs']] 
        n_insts = length(confs[['name']])
        return(n_insts)
    },
    get_appmix_myexpname = function()
    {
        json_data = load()

        confs = json_data[['exp_parameters']][['appconfs']] 
        appname = confs[['name']][[1]]
        n_insts = length(confs[['name']])

        discard = json_data[['exp_parameters']][['fs_discard']]
        discard = paste('discard', discard)

        fs = json_data[['exp_parameters']][['filesystem']]

        return(paste(appname, 'x', n_insts, fs, discard))
    },
    get_appmix_fs_discard = function()
    {
        json_data = load()
        dis = json_data[['exp_parameters']][['fs_discard']]

        if (is.null(dis)) {
            return('UNKNOWN')
        }
        return(dis)
    },
    get_do_wearleveling = function()
    {
        json_data = load()
        return( json_data[['do_wear_leveling']] )
    },
    get_n_gc_procs = function()
    {
        json_data = load()
        return( json_data[['n_gc_procs']] )
    },
    ending_ = function()
    {}
  )
)
 

RecorderJson <- setRefClass("RecorderJson",
  fields = list(json_data = "list"),
  methods = list(
    initialize = function(filepath='defaultfilepath')
    {
        if (filepath != 'defaultfilepath')
            json_data <<- read_json(filepath)
    },
    load = function()
    {
        # json_data <<- read_json(filepath)
    },
    get_write_traffic_size = function()
    {
        load()
        return(null_is_zero(json_data[['general_accumulator']][['traffic']][['write']]))
    },
    get_read_traffic_size = function()
    {
        load()
        return(null_is_zero(json_data[['general_accumulator']][['traffic']][['read']]))
    },
    get_discard_traffic_size = function()
    {
        load()
        return(null_is_zero(json_data[['general_accumulator']][['traffic']][['discard']]))
    },
    get_user_page_moves = function()
    {
        load()
        return(null_is_zero(json_data[['general_accumulator']][['gc']][['user.page.moves']]))
    },
    get_trans_page_moves = function()
    {
        load()
        return(null_is_zero(json_data[['general_accumulator']][['gc']][['trans.page.moves']]))
    },
    get_user_block_erase_cnt = function()
    {
        load()
        return(null_is_zero(json_data[['general_accumulator']][['gc']][['erase.data.block']]))
    },
    get_trans_block_erase_cnt = function()
    {
        load()
        return(null_is_zero(json_data[['general_accumulator']][['gc']][['erase.trans.block']]))
    },

    
    # time
    get_interest_workload_start_time = function()
    {
        load()
        timestamp = json_data[['interest_workload_start']]
        return(null_is_zero(timestamp))
    },
    get_gc_start_timestamp = function()
    {
        load()
        t = json_data[['gc_start_timestamp']]
        if (is.null(t)) {
            return(get_simulation_duration())
        } else {
            return(t)
        }
    },
    get_simulation_duration = function()
    {
        load()
        return(json_data[['simulation_duration']])
    },
    get_interest_workload_duration = function()
    {
        dur = (get_gc_start_timestamp() - get_interest_workload_start_time())/ SEC
    },
    get_gc_duration = function()
    {
        dur = (get_simulation_duration() - get_gc_start_timestamp())/ SEC
    },

    # summary
    get_write_bw = function()
    {
        load()
        write_mb = get_write_traffic_size() / MB
        dur = get_interest_workload_duration()
        return(write_mb/dur)
    },
    get_read_bw = function()
    {
        load()
        write_mb = get_read_traffic_size() / MB
        dur = get_interest_workload_duration()
        return(write_mb/dur)
    },

    fill_missing = function(count_list)
    {
        default = list('physical_read'=0,
                       'physical_write'=0,
                       'phy_block_erase'=0)
        default[names(count_list)] = count_list
        return(default)
    },
    fill_missing_merge_count = function(count_list)
    {
        default = list('switch_merge'=0,
                       'full_merge'=0,
                       'partial_merge'=0)
        default[names(count_list)] = count_list
        return(default)
    },
    get_nkftl_foreground_counts = function()
    {
        load()
        a = json_data[['general_accumulator']][['FORGROUND']]
        return(fill_missing(json_data[['general_accumulator']][['FORGROUND']]))
    },
    get_nkftl_fullmerge_counts = function()
    {
        load()
        return(fill_missing(json_data[['general_accumulator']][['FULL.MERGE']]))
    },
    get_nkftl_switchmerge_counts = function()
    {
        load()
        return(fill_missing(json_data[['general_accumulator']][['SWITCH.MERGE']]))
    },
    get_nkftl_simpleerase_counts = function()
    {
        load()
        return(fill_missing(json_data[['general_accumulator']][['SIMPLE.ERASE']]))
    },
    get_nkftl_partialmerge_counts = function()
    {
        load()
        return(fill_missing(json_data[['general_accumulator']][['PARTIAL.MERGE']]))
    },
    get_nkftl_merge_counts = function()
    {
        load()
        return(fill_missing_merge_count(
                json_data[['general_accumulator']][['garbage_collection']]))
    },
    get_valid_ratios = function()
    {
        load()
        ratios = json_data[['general_accumulator']][['victim_valid_ratio']]
        return(ratios)
    },
    get_contract_interest_workload_duration = function()
    {
        load()
        start = json_data[['interest_workload_start']]
        end = json_data[['interest_workload_end']]
        return(end - start)
    },
    get_contract_gc_duration = function()
    {
        load()
        return(json_data[['gc_duration_sec']])
    },
    get_contract_nonmerge_gc_duration = function()
    {
        load()
        return(null_is_zero(json_data[['non_merge_gc_duration_sec']]))
    },
    get_interest_foreground_write_bytes = function()
    {
        load()
        before = null_is_zero(
          json_data[['logical_ops_before_interest']][['write']])
        after = null_is_zero(
          json_data[['logical_ops_after_interest']][['write']])
        return(after - before)
    },
    get_interest_foreground_read_bytes = function()
    {
        load()
        before = null_is_zero(
          json_data[['logical_ops_before_interest']][['read']])
        after = null_is_zero(
          json_data[['logical_ops_after_interest']][['read']])
        return(after - before)
    },
    get_interest_flash_read_cnt = function()
    {
        load()
        before = null_is_zero(
          json_data[['flash_ops_before_interest']][['OP_READ']])
        after = null_is_zero(
          json_data[['flash_ops_after_interest']][['OP_READ']])
        return(after - before)
    },
    get_interest_flash_write_cnt = function()
    {
        load()
        before = null_is_zero(
          json_data[['flash_ops_before_interest']][['OP_WRITE']])
        after = null_is_zero(
          json_data[['flash_ops_after_interest']][['OP_WRITE']])
        return(after - before)
    },
    get_gc_flash_read_cnt = function()
    {
        load()
        before = null_is_zero(
          json_data[['flash_ops_before_gc']][['OP_READ']])
        after = null_is_zero(
          json_data[['flash_ops_after_gc']][['OP_READ']])
        return(after - before)
    },
    get_gc_flash_write_cnt = function()
    {
        load()
        before = null_is_zero(
          json_data[['flash_ops_before_gc']][['OP_WRITE']])
        after = null_is_zero(
          json_data[['flash_ops_after_gc']][['OP_WRITE']])
        return(after - before)
    },
    get_nonmerge_gc_flash_read_cnt = function()
    {
        load()
        before = null_is_zero(
          json_data[['flash_ops_before_non_merge_gc']][['OP_READ']])
        after = null_is_zero(
          json_data[['flash_ops_after_non_merge_gc']][['OP_READ']])
        return(after - before)
    },
    get_nonmerge_gc_flash_write_cnt = function()
    {
        load()
        before = null_is_zero(
          json_data[['flash_ops_before_non_merge_gc']][['OP_WRITE']])
        after = null_is_zero(
          json_data[['flash_ops_after_non_merge_gc']][['OP_WRITE']])
        return(after - before)
    },
    get_gc_flash_erase_cnt = function()
    {
        load()
        before = null_is_zero(
          json_data[['flash_ops_before_gc']][['OP_ERASE']])
        after = null_is_zero(
          json_data[['flash_ops_after_gc']][['OP_ERASE']])
        return(after - before)
    },
    get_nonmerge_gc_flash_erase_cnt = function()
    {
        load()
        before = null_is_zero(
          json_data[['flash_ops_before_non_merge_gc']][['OP_ERASE']])
        after = null_is_zero(
          json_data[['flash_ops_after_non_merge_gc']][['OP_ERASE']])
        return(after - before)
    },
    get_flash_ops_read = function()
    {
        load()
        ret = null_is_zero(json_data[['general_accumulator']][['flash_ops']][['OP_READ']])
        return(ret)
    },
    get_flash_ops_write = function()
    {
        load()
        ret = null_is_zero(json_data[['general_accumulator']][['flash_ops']][['OP_WRITE']])
        return(ret)
    },
    get_flash_ops_erase = function()
    {
        load()
        ret = null_is_zero(json_data[['general_accumulator']][['flash_ops']][['OP_ERASE']])
        return(ret)
    },

    get_hit_count = function()
    {
        load()
        ret = null_is_zero(json_data[['general_accumulator']][['Mapping_Cache']][['hit']])
        return(ret)
    },
    get_miss_count = function()
    {
        load()
        ret = null_is_zero(json_data[['general_accumulator']][['Mapping_Cache']][['miss']])
        return(ret)
    },
 
 
    get_interest_hit_cnt = function()
    {
        load()
        before = null_is_zero(
          json_data[['hitmiss_before_interest']][['hit']])
        after = null_is_zero(
          json_data[['hitmiss_after_interest']][['hit']])
        return(after - before)
    },
    get_interest_miss_cnt = function()
    {
        load()
        before = null_is_zero(
          json_data[['hitmiss_before_interest']][['miss']])
        after = null_is_zero(
          json_data[['hitmiss_after_interest']][['miss']])
        return(after - before)
    },
    get_gc_hit_cnt = function()
    {
        load()
        before = null_is_zero(
          json_data[['hitmiss_before_gc']][['hit']])
        after = null_is_zero(
          json_data[['hitmiss_after_gc']][['hit']])
        return(after - before)
    },
    get_gc_miss_cnt = function()
    {
        load()
        before = null_is_zero(
          json_data[['hitmiss_before_gc']][['miss']])
        after = null_is_zero(
          json_data[['hitmiss_after_gc']][['miss']])
        return(after - before)
    },
    get_valid_ratio_snapshots = function()
    {
        load()
        return(json_data[['ftl_func_valid_ratios']])
    },
    get_erasure_count_snapshots = function()
    {
        load()
        return(json_data[['ftl_func_erasure_count_dist']])
    },
    get_wlrunner_gc_start_timestamp = function()
    {
        load()
        return(json_data[['gc_start_timestamp']])
    },
    get_dftl_user_page_moves = function()
    {
        load()
        ret = null_is_zero(json_data[['general_accumulator']][['wearleveling']][['user.page.moves']])
        return(ret)
    },
    get_dftl_trans_page_moves = function()
    {
        load()
        ret = null_is_zero(json_data[['general_accumulator']][['wearleveling']][['trans.page.moves']])
        return(ret)
    },
    get_nkftl_page_moves = function()
    {
        load()
        ret = null_is_zero(json_data[['general_accumulator']][['wearleveling']][['physical_write']])
        return(ret)
    },
    get_ftlcounter_written_bytes = function()
    {
        load()
        ret = null_is_zero(json_data[['general_accumulator']][['traffic_size']][['write']])
        return(ret)
    },
    get_sim_write_bandwidth = function()
    {
        load()
        ret = null_is_zero(json_data[['write_bandwidth']])
        return(ret)
    },
    get_user_traffic_snapshots = function()
    {
        load()
        return(json_data[['ftl_func_user_traffic']])
    },
    get_gc_trigger_timestamp = function()
    {
        load()
        return(json_data[['gc_trigger_timestamp']])
    },
 

    ending_ = function()
    {}
  )
)

ChannelTimeline <- setRefClass("ChannelTimeline",
  fields = list(filepath = "character", df = "data.frame"),
  methods = list(
    init = function()
    {
        df <<- read.table(filepath, header = T)
    },
    organize = function()
    {
        d = df
        print(head(d))
        d = transform(d, channel = factor(channel))
        d = transform(d, start_time=start_time/(10^9))
        d = transform(d, end_time=end_time/(10^9))
        df <<- d
    },
    plot = function()
    {
        d = df
        p = ggplot(d, aes(x=start_time, xend=end_time,
                          y=channel, yend=channel,
                          color=op))+
            geom_segment(size=1)
        print(p)
    }, 
    main = function()
    {
        init()
        organize()
        plot()
    }
  )
)

TimelineRC <- setRefClass("TimelineRC",
  fields = list(filepath = "character", df = "data.frame"),
  methods = list(
    init = function()
    {
        df <<- read.table(filepath, header = T)
    },

    show = function() {
        print("-- TimelineRC Dataframe --")
        print(df)
    },

    organize = function() {
        d = df
        print(head(d))
        d = transform(d, start_time=start_time/(10^9))
        d = transform(d, end_time=end_time/(10^9))

        d = transform(d, op.index=interaction(op_id, op))

        d = ddply(d, .(op), transform, in_op_id = seq_along(op))

        d$op = factor(d$op, levels = c('write_ext',
                                       'read_ext',
                                       'read_trans_page',
                                       'prog_trans_page',
                                       'read_user_data',
                                       'write_user_data'))

        df <<- d
    },

    plot_separate_facet = function() {
        d = df
        p = ggplot(d, aes(x=start_time, xend=end_time,
                          y=in_op_id, yend=in_op_id,
                          color = op
                          )) +
            geom_segment(position='dodge', size=2) + 
            facet_grid(op~., space = 'free_y')
        print(p)
    },

    plot_one_facet = function() {
        d = df
        p = ggplot(d, aes(x=start_time, xend=end_time,
                          y=op_id, yend=op_id,
                          color = op
                          )) +
            geom_segment(position='dodge', size=2)
        print(p)
    },

    plot = function() {
        plot_one_facet()
        # plot_separate_facet()
    },

    main = function() {
        init()
        organize()
        plot()
    }
  )
)

BlkParseEvents <- setRefClass("BlkParseEvents",
  fields = list(filepath = "character", df = "data.frame"),
  methods = list(
    load = function()
    {
        # d = read.table(filepath, header = F)
        # names(d) = c('pid', 'operation', 'offset', 'size', 'timestamp', 'pre_wait_time',
                     # 'sync')
        # return(d)
        d = load_file_from_cache(filepath, 'load_event_file')
        return(d)
    }
  )
)

FioReport <- setRefClass("FioReport",
  fields = list(filepath = "character", df = "data.frame"),
  methods = list(
    load = function()
    {
        jsondata = read_json(filepath)
        return(jsondata)
    },
    get_read_bw = function()
    {
        jsondata = load()
        return(jsondata[['jobs']][['read']][['bw']])
    },
    get_write_bw = function()
    {
        jsondata = load()
        return(jsondata[['jobs']][['write']][['bw']])
    }
  )
)

FioBwLog <- setRefClass("FioBwLog",
  fields = list(filepath = "character", df = "data.frame"),
  methods = list(
    load = function()
    {
        d = read.csv(file=filepath, header=F)
        names(d) = c("time", "bw", "direction", "blocksize")
        return(d)
    }
  )
)

LeveldbOutParsed <- setRefClass("LeveldbOutParsed",
  fields = list(filepath = "character", df = "data.frame"),
  methods = list(
    load = function()
    {
        d = read.table(file=filepath, header=T, sep=';')
        return(d)
    }
  )
)

GcLogParsed <- setRefClass("GcLogParsed",
  fields = list(filepath = "character", df = "data.frame"),
  methods = list(
    load = function()
    {
        d = load_file_from_cache(filepath, 'load_gclogparsed')
        return(d)
    }
  )
)

ExtentTable <- setRefClass("ExtentTable",
  fields = list(filepath = "character", df = "data.frame"),
  methods = list(
    load = function()
    {
        d = read.table(file=filepath, header=T, sep=';')
        print('here')
        return(d)
    }
  )
)

StraceTable <- setRefClass("StraceTable",
  fields = list(filepath = "character", df = "data.frame"),
  methods = list(
    load = function()
    {
        d = read.table(file=filepath, header=T)
        return(d)
    }
  )
)

StatsJson <- setRefClass("StatsJson",
  fields = list(filepath = "character", json_data = "list"),
  methods = list(
    load = function()
    {
        json_data <<- read_json(filepath)
    },
    get_write_traffic_size = function()
    {
        load()
        return(null_is_zero(json_data[['written_bytes']]))
    },
    get_disk_used_bytes = function()
    {
        load()
        return(null_is_zero(json_data[['disk_used_bytes']]))
    }
  )
)

LpnCount <- setRefClass("LpnCount",
  fields = list(filepath = "character"),
  methods = list(
    load = function()
    {
        d = read.table(file=filepath, header=T)
        return(d)
    }
  )
)


LpnSem <- setRefClass("LpnSem",
  fields = list(filepath = "character"),
  methods = list(
    load = function()
    {
        d = read.table(file=filepath, header=T, sep=';')
        return(d)
    }
  )
)


DirtyTable <- setRefClass("LpnSem",
  fields = list(filepath = "character"),
  methods = list(
    load = function()
    {
        d = read.table(file=filepath, header=T, sep=';')
        return(d)
    }
  )
)


NcqDepthTimelineTable <- setRefClass("NcqDepthTimelineTable",
  fields = list(filepath = "character"),
  methods = list(
    load = function()
    {
        d = read.table(file=filepath, header=T, sep=';')
        levels(d$operation) = plyr::revalue(levels(d$operation), 
                c('OP_READ'='read', 'OP_WRITE'='write', 'OP_DISCARD'='discard'))
        return(d)
    }
  )
)



AppTimeTxt <- setRefClass("AppTimeTxt",
  fields = list(filepath = "character"),
  methods = list(
    load = function()
    {
        v = scan(file=filepath)
        return(v)
    }
  )
)



