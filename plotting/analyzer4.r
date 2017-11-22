# libraries
library(ggplot2)
library(plyr)
library(dplyr)
library(reshape2)
library(gridExtra)
library(jsonlite)
library(digest)

source('doraemon/header.r')
source('doraemon/file_parsers.r')

# copy the following so you can do sme()
# setwd(WORKDIRECTORY)
sme <- function()
{
    WORKDIRECTORY= "/Users/junhe/workdir/analysis-script/"
    THISFILE     ='doraemon/analyzer4.r'
    setwd(WORKDIRECTORY)
    source(THISFILE)
}

ddply_calc_bw_by_op <- function(d) {
    op_type = unique(d$operation)[1]
    if (op_type == 'OP_READ') {
        d = transform(d, bw_util=bw/max_r_bw)
    } else if (op_type == 'OP_WRITE') {
        d = transform(d, bw_util=bw/max_w_bw)
    }
    return(d)
}

rename_operation <- function(operation)
{
    op = revalue(operation, c("lba_read"    = "logical_read",
                              "lba_write"   = "logical_write",
                              "lba_discard" = "logical_discard",
                              "page_read"   = "physical_read",
                              "page_write"  = "physical_write",
                              "block_erase" = "phy_block_erase"))
    return(op)
}

get_color_map <- function()
{
    map = c()
    tags = c("write_user", "read_user", "read_trans", 
             "prog_trans",
             "read.data.gc", "write.data.gc", 
             "erase.data.gc")
    idx = 1 
    for (tag in tags) {
        map[tag] = cbPalette[idx]
        idx = idx + 1
    }
    return(map)
}




simple_hist <- function(d, xstr, ystr, fillstr=NULL, title='default-title',
                        position='dodge')
{
    if (is.null(fillstr)) {
        p = ggplot(d, aes_string(x=xstr, y=ystr)) 
    } else {
        p = ggplot(d, aes_string(x=xstr, y=ystr, fill=fillstr, 
                                 order=paste('desc(', fillstr, ')', sep='')))
    }
        p = p +
        geom_bar(stat='identity', position=position) +
        ggplot_common_addon() +
        ggtitle(title)
    return(p)
}


pls_classify <- function(blk, d)
{
    after_start = blk >= d$start
    before_end = blk <= d$end
    both = after_start & before_end

    if (!any(both)) {
        return('data')
    } else {
        return(as.character(d$type[both]))
    }
}

# Mark sequential accesses
mark_seq <- function(d, remove_dup=TRUE)
{
    # remove dups
    if (remove_dup == TRUE) {
        dup = duplicated(d$offset)
        d = d[!dup, ]
    }

    d = transform(d, end = offset+size)

    ends = d$end
    ends = ends[-length(ends)]
    d$previous_end = c(NA, ends)
    d$is_seq = d$previous_end == d$offset
    d$distance_to_prev_end = d$offset - d$previous_end

    return(d)
}


c_______general_organizer_______ <- function() {}


c_______file_parsers____________ <- function(){}


c_______sub_analyser____________ <- function(){}

SubExpBandwidth <- setRefClass("SubExpBandwidth",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            recjson = RecorderJson(filepath=paste(subexppath, 'recorder.json', sep='/'))
            confjson = ConfigJson(filepath=paste(subexppath, 'config.json', sep='/'))

            page_size = confjson$get_page_size()
            block_size = confjson$get_block_size()

            gc_user_data_size_moved = recjson$get_user_page_moves() * page_size
            gc_trans_data_size_moved = recjson$get_trans_page_moves() * page_size
                     
            gc_user_block_size_erased=recjson$get_user_block_erase_cnt() * as.numeric(block_size)
            gc_trans_block_size_erased=recjson$get_trans_block_erase_cnt() * block_size

            return(list(
                     write_bw=recjson$get_write_bw(),
                     read_bw=recjson$get_read_bw(),
                     discarded_bytes=recjson$get_discard_traffic_size(),
                     read_bytes=recjson$get_read_traffic_size(),
                     write_bytes=recjson$get_write_traffic_size(),
                     user_traffic_size=confjson$get_user_traffic_size(),
                     patternclass=confjson$get_pattern_name(),
                     stripe_size=confjson$get_stripe_size(),
                     ext4hasjournal=confjson$get_ext4hasjournal(),
                     gc_dur=recjson$get_gc_duration(),
                     ncq_depth=confjson$get_ncq_depth(),
                     chunk_size=confjson$get_chunk_size(),
                     gc_user_data_size_moved=gc_user_data_size_moved,
                     gc_trans_data_size_moved=gc_trans_data_size_moved,
                     gc_user_block_size_erased=gc_user_block_size_erased,
                     gc_trans_block_size_erased=gc_trans_block_size_erased,
                     cache_mapped_data_bytes=confjson$get_cache_mapped_data_bytes(),
                     preallocate=confjson$get_preallocate(),
                     interest_workload_duration=recjson$get_interest_workload_duration(),
                     keep_size=confjson$get_keep_size(),
                     filesystem=confjson$get_filesystem(),
                     blocksize=confjson$get_block_size(),
                     leveldb_n_inst=confjson$get_leveldb_n_instances(),
                     expname=confjson$get_expname()
                     ))
        }
    )
)
 
SubExpBlkParseEvents <- setRefClass("SubExpBlkParseEvents",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            print(subexppath)

            confjson = create_configjson(subexppath=subexppath)
            events_obj = BlkParseEvents(filepath=paste(subexppath, 'blkparse-events-for-ftlsim.txt', sep="/"))

            fs = confjson$get_filesystem()
            segment_bytes = confjson$get_segment_bytes()
            bench_to_run = confjson$get_bench_to_run()
            # if ( ! (fs == 'f2fs' && segment_bytes == 128*KB && bench_to_run == 'test-insert-rand') ) {
                # return()
            # }

            d = events_obj$load()

            title = paste(
                          confjson$get_leveldb_n_instances(),
                          confjson$get_benchmarks(),
                          confjson$get_filesystem())
            # elems = unlist(strsplit(subexppath, '/', fixed=T))
            # title = elems[length(elems)]
            analysis_set(d, title)
        },
        analysis_set = function(d, title)
        {
            plot_time_space(d, title=title, colorstr='operation')
        },
        plot_time_space = function(d, title='', colorstr='operation')
        {
            title = paste(title, 'time space', colorstr)

            if (colorstr == 'blk_type') {
                d = add_blk_type(d)
            }

            # d = subset(d, timestamp < 5e8)

            logical_block = 1592
            print(time_space_graph(d, title, colorstr, 
                   range=c(logical_block * 128*KB, (logical_block+1) * 128*KB)))
            # print(time_space_graph(d, title, colorstr, range=c(138*MB, 150*MB)))
            # print(time_space_graph(d, title, colorstr, range=c(4224*2*KB, 4224*2*KB+128*KB)))
            # print(time_space_graph(d, title, colorstr))
            return()

            extents = ExtentTable(
                  filepath=paste(subexppath, 'extents.json.table', sep='/'))
            d.ext = extents$load()

            file_paths = unique(d.ext$file_path)
            for (file_path in file_paths) {
                print('-----------------')
                print(file_path)
                range = get_file_range(d.ext, file_path)
                range = range * 4 *KB
                print(range)
                print_plot(time_space_graph(d, title=file_path, colorstr, range, file_path))
            }

        },
        time_space_graph = function(d, title='default title', 
            colorstr='operation', range=NA, filepath='')
        {
            print(range)

            # d = subset(d, timestamp > 3 & timestamp < 3.5)
            # d = subset(d, offset < 125 & offset > 45)
            # print(d)

            size_unit = MB

            # d = transform(d, offset=offset/(4*KB), size=size/(4*KB))
            d = transform(d, offset=offset/size_unit, size=size/size_unit)
            range = range/size_unit
            if (nrow(d) == 0) {
                print(paste(filepath, 'has zero'))
                return()
            }

            p = ggplot(d, aes_string(color=colorstr)) +
                geom_segment(aes(x=timestamp, xend=timestamp, y=offset, 
                                 yend=offset+size), size=5)+
                geom_point(aes(x=timestamp, y=offset)) +
                # scale_y_continuous(breaks=seq(0, 1024, 16), limits=c(0, 32)) +
                geom_hline(y=range[1]) +
                geom_hline(y=range[2]) +
                ylab('address') +
                # xlab('seq id') +
                ggtitle(title)

            if (! is.na(range)) {
                d = subset(d, range[1] <= offset & offset <= range[2] )
                padding = range[2] - range[1]
                p = p + coord_cartesian(ylim=c(range[1] - padding, range[2] + padding))
            }

            if (colorstr == 'operation') {
                p = p + scale_color_manual(
                   values=get_color_map2(c("read", "write", "discard")))
            }
            return(p)
        }
    )
)


SubExpNkftlStats <- setRefClass("SubExpNkftlStats",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            confjson = create_configjson(subexppath=subexppath)
            recjson = RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))
            
            return(list(
                foreground=recjson$get_nkftl_foreground_counts(),
                fullmerge=recjson$get_nkftl_fullmerge_counts(),
                switchmerge=recjson$get_nkftl_switchmerge_counts(),
                merge=recjson$get_nkftl_merge_counts(),
                blocksize=confjson$get_block_size(),
                pagesize=confjson$get_page_size(),
                filesystem=confjson$get_filesystem(),
                nkftl_n=confjson$get_nkftl_n(),
                nkftl_k=confjson$get_nkftl_k()
                ))
        }
    )
)

SubExpValidRatios <- setRefClass("SubExpValidRatios",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            confjson = create_configjson(subexppath=subexppath)
            recjson = RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            ratios = recjson$get_valid_ratios()
            d = data.frame(count=unlist(ratios))
            d$valid_ratio = as.numeric(rownames(d))

            d = transform(d, filesystem=confjson$get_filesystem())
            d = transform(d, blocksize=confjson$get_block_size())
            return(d)
        }
    )
)
 

SubExpExtents <- setRefClass("SubExpExtents",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            path = paste(subexppath, 'extents.txt', sep='/')
            print(path)
            tab = ExtentTable(filepath=path)
            d = tab$load()
            
            plot(d)
        },
        plot = function(d)
        {
            d = transform(d, 
                          Physical_start = Physical_start * 4096/MB,
                          Physical_end = Physical_end * 4096/MB)

            p = ggplot(d, aes(color=dirpath)) +
                geom_segment(aes(x=Physical_start, xend=Physical_end,
                                 y=1, yend=1)) +
                scale_x_continuous(breaks=seq(0, 1024, 2)) +
                ggplot_common_addon()
            print(p)
        }
    )
)


SubExpContract <- setRefClass("SubExpContract",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            recjson = RecorderJson(filepath=paste(subexppath, 'recorder.json', sep='/'))
            confjson = ConfigJson(filepath=paste(subexppath, 'config.json', sep='/'))

            page_size = confjson$get_page_size()
            block_size = confjson$get_block_size()

            ret = list(
                    interest_wl_dur=recjson$get_contract_interest_workload_duration(),
                    traffic_size=confjson$get_contractbench_traffic_size(),
                    space_size=confjson$get_contractbench_space_size(),
                    gc_duration=recjson$get_contract_gc_duration(),
                    nonmerge_gc_duration=recjson$get_contract_nonmerge_gc_duration(),
                    expname=confjson$get_expname(),
                    interest_fg_read_bytes=recjson$get_interest_foreground_read_bytes(),
                    interest_fg_write_bytes=recjson$get_interest_foreground_write_bytes(),
                    interest_flash_read_bytes=recjson$get_interest_flash_read_cnt()*page_size,
                    interest_flash_write_bytes=recjson$get_interest_flash_write_cnt()*page_size,
                    interest_hit_cnt=recjson$get_interest_hit_cnt(),
                    interest_miss_cnt=recjson$get_interest_miss_cnt(),
                    gc_flash_read_bytes=recjson$get_gc_flash_read_cnt()*page_size,
                    gc_flash_write_bytes=recjson$get_gc_flash_write_cnt()*page_size,
                    gc_flash_erase_bytes=recjson$get_gc_flash_erase_cnt()*block_size,
                    nonmerge_gc_flash_read_bytes=recjson$get_nonmerge_gc_flash_read_cnt()*page_size,
                    nonmerge_gc_flash_write_bytes=recjson$get_nonmerge_gc_flash_write_cnt()*page_size,
                    nonmerge_gc_flash_erase_bytes=recjson$get_nonmerge_gc_flash_erase_cnt()*block_size,
                    gc_hit_cnt=recjson$get_gc_hit_cnt(),
                    gc_miss_cnt=recjson$get_gc_miss_cnt(),
                    grouping=confjson$get_contractbench_grouping(),
                    chunk_size=confjson$get_contractbench_chunk_size(),
                    operation=confjson$get_contractbench_operation(),
                    ncq_depth=confjson$get_ncq_depth(),
                    stripe_size=confjson$get_stripe_size(),
                    ftl_type=confjson$get_ftl_type(),
                    page_read_time=confjson$get_page_read_time(),
                    page_prog_time=confjson$get_page_prog_time(),
                    block_erase_time=confjson$get_block_erase_time(),
                    page_size=page_size,
                    block_size=block_size,
                    n_channels_per_dev=confjson$get_n_channels_per_dev(),
                    is_aligned=confjson$get_contractbench_is_aligned()

                     )
            for (key in names(ret)) {
                if (is.null(ret[[key]])) {
                    ret[[key]] = NA
                }
            }
            return(ret)
        }
    )
)
 

SubExpAlignment <- setRefClass("SubExpNkftlStats",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            confjson = create_configjson(subexppath=subexppath)
            recjson = RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            pagesize=as.numeric(confjson$get_page_size())
            blocksize=as.numeric(confjson$get_block_size())
            
            return(list(
                merge=recjson$get_nkftl_merge_counts(),
                blocksize=confjson$get_block_size(),
                pagesize=confjson$get_page_size(),
                filesystem=get_alignment_filesystem(confjson),
                nkftl_n=confjson$get_nkftl_n(),
                nkftl_k=confjson$get_nkftl_k(),
                segment_bytes=confjson$get_segment_bytes(),
                ncq_depth=confjson$get_ncq_depth(),
                discarded_bytes=recjson$get_discard_traffic_size(),
                read_bytes=recjson$get_read_traffic_size(),
                write_bytes=recjson$get_write_traffic_size(),
                fullmerge_ops=recjson$get_nkftl_fullmerge_counts(),
                switchmerge_ops=recjson$get_nkftl_switchmerge_counts(),
                partialmerge_ops=recjson$get_nkftl_partialmerge_counts(),
                simpleerase_ops=recjson$get_nkftl_simpleerase_counts(),
                bench_to_run=confjson$get_bench_to_run(),
                benchname=get_benchname(confjson),
                ext4hasjournal=confjson$get_ext4hasjournal(),
                flash_w_bytes=recjson$get_flash_ops_write() * pagesize,
                flash_r_bytes=recjson$get_flash_ops_read() * pagesize,
                flash_e_bytes=recjson$get_flash_ops_erase() * blocksize
                ))
        },
        get_benchname = function(confjson)
        {
            wlclass = confjson$get_workload_class()
            if (wlclass == 'Leveldb') {
                return('LevelDB')
            } else if (wlclass == 'Sqlbench') {
                return(confjson$get_bench_to_run())
            } else if (wlclass == 'Varmail') {
                return('Varmail')
            } else if (wlclass == 'Sqlite') {
                return(paste('Sqlite', confjson$get_sqlite_pattern(), sep='-'))
            }
        },
        get_alignment_filesystem = function(confjson)
        {
            fs = confjson$get_exp_parameters_filesystem()
            return(fs)
        }
    )
)


SubExpSemantics <- setRefClass("SubExpSemantics",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            confjson = create_configjson(subexppath=subexppath)
            recjson = RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            fs = confjson$get_filesystem()
            segment_bytes = confjson$get_segment_bytes()
            if (fs != 'ext4')
                return()
 

            gclog = GcLogParsed(
                  filepath=paste(subexppath, 'gc.log.parsed', sep='/'))
            d = gclog$load()
            d = transform(d, semantics=trim(semantics),
                             valid=as.logical(trim(valid)))

            sizes = get_data_size_moved(d)
            print(sizes)

            pagesize=as.numeric(confjson$get_page_size())
            blocksize=as.numeric(confjson$get_block_size())
            
            ret = list(
                blocksize=confjson$get_block_size(),
                pagesize=confjson$get_page_size(),
                filesystem=get_alignment_filesystem(confjson),
                segment_bytes=confjson$get_segment_bytes(),
                bench_to_run=confjson$get_bench_to_run(),
                benchname=get_benchname(confjson)
                )
            ret = append(ret, sizes)
            return(ret)
        },
        get_benchname = function(confjson)
        {
            wlclass = confjson$get_workload_class()
            if (wlclass == 'Leveldb') {
                return('LevelDB')
            } else if (wlclass == 'Sqlbench') {
                return(confjson$get_bench_to_run())
            } else if (wlclass == 'Varmail') {
                return('Varmail')
            } else if (wlclass == 'Sqlite') {
                return(paste('Sqlite', confjson$get_sqlite_pattern(), sep='-'))
            }
        },
        get_alignment_filesystem = function(confjson)
        {
            fs = confjson$get_exp_parameters_filesystem()
            return(fs)
        },
        get_data_size_moved = function(d)
        {
            n = nrow(d)
            for (i in seq(1, n)) {
                sem = d[i, 'semantics']
                metas = c(
                    'superblock',
                    'groupdesc',
                    'reserved-gdt',
                    'block-bitmap',
                    'inode-bitmap',
                    'inode-table',
                    'journal'
                    )
                if (sem %in% metas) {
                    # d[i, 'semantics'] = 'metadata'
                    d[i, 'semantics'] = sem
                } else if (sem == 'None') {
                    d[i, 'semantics'] = 'None'
                } else {
                    d[i, 'semantics'] = 'file data'
                }
            }

            d = transform(d, size=2*KB)
            d = aggregate(size~semantics, data=d, sum)

            l = as.vector(d$size)
            names(l) = as.vector(d$semantics)
            return(l)
        }
    )
)




SubExpGcLogParsedNkftl <- setRefClass("SubExpGcLogParsedNkftl",
    fields = list(subexppath="character", blocksize="numeric"),
    methods = list(
        run = function()
        {
            # load the objects
            confjson = create_configjson(subexppath=subexppath)
            recjson = RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))
            blocksize <<- confjson$get_block_size()

            fs = confjson$get_filesystem()
            seg_bytes = confjson$get_segment_bytes()
            if (fs != 'ext4' || seg_bytes != 128*KB) {
                return()
            }


            plot_semantic_graphs()
            # plot_region_time_space_graph()
            # plot_file_allocation()

            # plot_syscall_pattern()
        },

        plot_syscall_pattern = function()
        {
            d0 = get_data_of_file(0)
            d1 = get_data_of_file(1)

            d = rbind(d0, d1)
            # plot_intra_file_hist(d)
            plot_time_sequence(d)
        },
        get_data_of_file = function(inst_id) 
        {
            tracefile = paste(inst_id, '.strace.out.table', sep='')
            datafilepath = paste('/mnt/fsonloop/sqlite_dir/inst-', inst_id, '/data.db', sep='')

            strace_table = StraceTable(filepath=paste(subexppath, tracefile , sep="/"))
            d = strace_table$load()

            d = subset(d, filepath == datafilepath)
            d = subset(d, callname %in% c('write'))
            return(d)
        },
        plot_intra_file_hist = function(d)
        {
            p = ggplot(d, aes(x=offset/MB)) +
                geom_bar() +
                facet_grid(filepath~.)
            print_plot(p)
        },
        plot_time_sequence = function(d)
        {
            ddply_time_offset <- function(d)
            {
                min_time = min(d$time)
                d = transform(d, time_offset = time - min_time)
                return(d)
            }

            d = ddply(d, .(filepath), ddply_time_offset)
            d$pattern = revalue(d$filepath, c(
                                              '/mnt/fsonloop/sqlite_dir/inst-0/data.db'='Random',
                                              '/mnt/fsonloop/sqlite_dir/inst-1/data.db'='Sequential'))


            d = transform(d, offset = offset / KB)
            p = ggplot(d, aes(x=time_offset, y=offset)) +
                # geom_segment(aes(xend=time, yend=offset+length), size=5) +
                geom_point() +
                scale_y_continuous(breaks=seq(0, 1024, 64)) +
                facet_grid(~pattern) +
                xlab('Time') +
                ylab('File Offset (KB)') +
                theme_zplot() +
                theme(panel.margin.x = unit(1, "lines"))
            print_plot(p)
            save_plot(p, 'sqlite-file-pattern', h=3, w=6, save=T)
        },

        plot_region_time_space_graph = function()
        {
            events_obj = BlkParseEvents(filepath=paste(subexppath, 'blkparse-events-for-ftlsim.txt', sep="/"))
            d.events = events_obj$load()

            plot_time_space(d.events)
            # plot_time_space(d.events, logical_block=1368)
            # plot_time_space_by_file(d.events)

            # for (block in c(1528, 1752, 1912, 2137, 2169, 2264, 2328)) {
                # plot_time_space(d.events, title=paste(block),
                                # logical_block=block)
            # }
        },

        plot_semantic_graphs = function()
        {
            gclog = GcLogParsed(
                  filepath=paste(subexppath, 'gc.log.parsed', sep='/'))
            d = gclog$load()
            d = transform(d, semantics=trim(semantics),
                             valid=as.logical(trim(valid)))

            d = transform(d, offset=lpn * 2048 / MB)
            d = transform(d, logical_block = (lpn * 2*KB) %/% blocksize)

            # plot_sem_blocknum(d)
            # plot_sem_vs_lpn(d)
            # plot_sem_vs_segment(d)
            # plot_semantic_size(d)
            plot_semantic_size_paper(d)
        },

        plot_file_allocation = function()
        {
            extents = ExtentTable(
                  filepath=paste(subexppath, 'extents.json.table', sep='/'))
            d = extents$load()
            d = transform(d, offset_start = Physical_start * 4096 + 8*MB)
            d = transform(d, offset_end = (Physical_end + 1) * 4096 + 8*MB)
            d = transform(d, size = Length * 4096)

            # d = subset(d, offset_start > 125*MB)
            # d = subset(d, file_path %in% c('sqlite_dir/inst-0/data.db',
                                           # 'sqlite_dir/inst-1/data.db' 
                                           # ))
            print(d)

            unit_type = 'MB'

            if (unit_type == 'MB') {
                unit_size = MB
            } else if (unit_type == 'page') {
                unit_size = 2 * KB
            }

            d = transform(d, start = offset_start / unit_size)
            d = transform(d, end = offset_end / unit_size)

            d = arrange(d, Logical_start)

            d = ddply(d, .(file_path), transform, ext_i=seq_along(Logical_start))
            
            d = subset(d, start > 132)

            p = ggplot(d, aes(x=start, xend=end,
                              y=ext_i, yend=ext_i,
                              color=file_path 
                              )) +
                geom_segment(size=5) +
                geom_text(aes(label=size/1024), color='black') +
                geom_point() +
                scale_x_continuous(breaks=seq(0, 200)) +
                xlab(unit_type) +
                # facet_grid(file_path~.) +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) 
            print_plot(p)
        },










        plot_time_space = function(d, title='', logical_block=NA)
        {
            if (! is.na(logical_block)) {
                range=c(logical_block * 128*KB, (logical_block+1) * 128*KB)
                print_plot(time_space_graph(d, title, colorstr='operation', 
                   range=range))
            } else {
                print(time_space_graph(d, title, colorstr='operation'))
            }
        },
        plot_time_space_by_file = function(d)
        {
            extents = ExtentTable(
                  filepath=paste(subexppath, 'extents.json.table', sep='/'))
            d.ext = extents$load()

            # file_paths = unique(d.ext$file_path)
            file_paths = c("sqlite_dir/inst-0/data.db")
            for (file_path in file_paths) {
                print('-----------------')
                print(file_path)
                range = get_file_byte_range(d.ext, file_path)
                range = range
                print(range)
                print_plot(
                   time_space_graph(d=d, title=file_path, range=range))
            }
        },
        time_space_graph = function(d, title='default title', 
            colorstr='operation', range=NA)
        {
            size_unit = MB
            d = transform(d, offset=offset/size_unit, size=size/size_unit)
            range = range/size_unit

            d = subset(d, timestamp < 500000000)

            p = ggplot(d, aes_string(color=colorstr)) +
                geom_segment(aes(x=timestamp, xend=timestamp, y=offset, 
                                 yend=offset+size), size=5)+
                geom_point(aes(x=timestamp, y=offset)) +
                ylab('address') +
                # xlab('seq id') +
                ggtitle(title)

            if (length(range) == 2) {
                # d = subset(d, range[1] <= offset & offset <= range[2] )
                padding = range[2] - range[1]
                p = p + 
                    coord_cartesian(
                        ylim=c(range[1] - padding, range[2] + padding)) +
                    geom_hline(y=range[1]) +
                    geom_hline(y=range[2])
            }

            if (colorstr == 'operation') {
                p = p + scale_color_manual(
                   values=get_color_map2(c("read", "write", "discard")))
            }
            return(p)
        },

        get_file_byte_range = function(d.ext, path)
        {
            d = subset(d.ext, file_path == path)

            start_block = min(d$Physical_start)
            end_block = max(d$Physical_end)
            start = (start_block * 4 * KB + (8 * MB))
            end = ((end_block + 1) * 4 * KB + (8 * MB))
            return(c(start, end))
        },
        plot_semantic_size = function(d)
        {
            d = transform(d, size=2*KB)
            d = aggregate(size~semantics, data=d, sum)

            print(d)
            p = ggplot(d, aes(x=semantics, y=size/KB)) +
                geom_bar(stat='identity') +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            print_plot(p)
        },
        plot_semantic_size_paper = function(d)
        {
            print(head(d))
            n = nrow(d)
            for (i in seq(1, n)) {
                sem = d[i, 'semantics']
                if (substr(sem, 1, 7) == 'varmail') {
                    d[i, 'semantics'] = 'file data'
                } else {
                    d[i, 'semantics'] = 'metadata'
                }
            }

            d = transform(d, size=2*KB)
            d = aggregate(size~semantics, data=d, sum)

            p = ggplot(d, aes(x=semantics, y=size/MB)) +
                geom_bar(stat='identity') +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            print_plot(p)
        },
        check_alignment = function(d, d.ext)
        {
            ddply_calc_offset <- function(d, range)
            {
                lpn_min = min(d$lpn)
                lpn_aligned = (lpn_min %/% 64) * 64

                phy_block = min(d$blocknum)
                ppn_aligned = phy_block * 64

                d = transform(d, lpn_off = lpn - lpn_aligned)
                d = transform(d, ppn_off = ppn - ppn_aligned)
                d = transform(d, dist_to_end = range[2] - lpn)

                d = arrange(d, ppn)

                return(d)
            }

            filepath = 'leveldb_data0/000029.ldb'
            # filepath = 'leveldb_data0/MANIFEST-000002'
            # filepath = 'inode-table'
            # range=c(1,1)
            # filepath = 'leveldb_data0/LOG'

            # range = get_file_range(d.ext, path=filepath)

            d = subset(d, semantics == filepath)

            d = ddply(d, .(blocknum), ddply_calc_offset, range)
            print(d)
        },
        plot_sem_blocknum = function(d)
        {
            p = ggplot(d, aes(x=factor(blocknum), y=factor(semantics))) +
                geom_point() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) 
            print_plot(p)
        },
        plot_sem_vs_lpn = function(d)
        {
            p = ggplot(d, aes(x=lpn * 2048/MB, y=factor(semantics))) +
                geom_point() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) 
            print_plot(p)
        },
        plot_sem_vs_segment = function(d)
        {
            d = transform(d, seg_id = (lpn * 2048) %/% (128*KB))
            p = ggplot(d, aes(x=factor(seg_id), y=factor(semantics))) +
                geom_point() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) 
            print_plot(p)
        }
 
    )
)


SubExpFileSnake <- setRefClass("SubExpFileSnake",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            confjson = create_configjson(subexppath=subexppath)
            recjson = RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            pagesize=as.numeric(confjson$get_page_size())
            blocksize=as.numeric(confjson$get_block_size())
            
            return(list(
                merge=recjson$get_nkftl_merge_counts(),
                blocksize=confjson$get_block_size(),
                pagesize=confjson$get_page_size(),
                filesystem=confjson$get_exp_parameters_filesystem(),
                nkftl_n=confjson$get_nkftl_n(),
                nkftl_k=confjson$get_nkftl_k(),
                segment_bytes=confjson$get_segment_bytes(),
                ncq_depth=confjson$get_ncq_depth(),
                discarded_bytes=recjson$get_discard_traffic_size(),
                read_bytes=recjson$get_read_traffic_size(),
                write_bytes=recjson$get_write_traffic_size(),
                fullmerge_ops=recjson$get_nkftl_fullmerge_counts(),
                switchmerge_ops=recjson$get_nkftl_switchmerge_counts(),
                partialmerge_ops=recjson$get_nkftl_partialmerge_counts(),
                simpleerase_ops=recjson$get_nkftl_simpleerase_counts(),
                ext4hasjournal=confjson$get_ext4hasjournal(),
                flash_w_bytes=recjson$get_flash_ops_write() * pagesize,
                flash_r_bytes=recjson$get_flash_ops_read() * pagesize,
                flash_e_bytes=recjson$get_flash_ops_erase() * blocksize,
                file_size=confjson$get_filesnake_file_size(),
                write_pre_file=confjson$get_filesnake_write_pre_file()
                ))
        }
    )
)



c_______benchmark_classes__________ <- function(){}

BlkParseEventsBench <- setRefClass("BlkParseEventsBench",
    fields = list(),
    methods = list(
        run = function(exp_rel_path)
        {
            subexpiter = SubExpIter(exp_rel_path=exp_rel_path)
            result = subexpiter$iter_each_subexp(SubExpBlkParseEvents)
        }
    )
)

LocalityBench <- setRefClass("LocalityBench",
    fields = list(expname='character'),
    methods = list(
        run = function(exp_rel_path, testclass)
        {
            d = aggregate_subexp_list_to_dataframe(exp_rel_path, 
                                               SubExpContract)

            d = cols_to_num(d, c('interest_wl_dur',
                                 'traffic_size',
                                 'space_size',
                                 'gc_duration',
                                 'chunk_size',
                                 'interest_fg_read_bytes', 'interest_fg_write_bytes',
                                 'interest_flash_read_bytes', 'interest_flash_write_bytes', 
                                 'gc_flash_read_bytes', 'gc_flash_write_bytes', 'gc_flash_erase_bytes',
                                 'interest_hit_cnt', 'interest_miss_cnt',
                                 'gc_hit_cnt', 'gc_miss_cnt',
                                 'page_size', 'block_size', 'n_channels_per_dev',
                                 'page_read_time', 'page_prog_time', 'block_erase_time',
                                 'nonmerge_gc_flash_read_bytes', 'nonmerge_gc_flash_write_bytes', 
                                 'nonmerge_gc_flash_erase_bytes', 'nonmerge_gc_duration'
                                 ))

            d = transform(d, interest_wl_dur=interest_wl_dur/SEC)
            d = transform(d, traffic_size=traffic_size/MB)
            d = transform(d, ftl_type=revalue(ftl_type, c('dftldes'='PAGE-FTL', 'nkftl2'='HYBRID-FTL')))
            d = add_max_rw_bw(d)
            if ('space_size' %in% names(d))
                d = transform(d, space_size=space_size/MB)


            if (testclass == 'scale-size') {
                # ----------- scale size ----------------------------------
                benchname='requestscale-size-non-seg'
                # save=T
                save=F
                ftl_type = get_ftl_type(d)
                plot_requestscale_size_imm_bw(d, benchname, save, ftl_type)

            } else if (testclass == 'scale-count') {
                # ----------- scale count ----------------------------------
                benchname='requestscale-count-non-seg'
                # save=T
                save=F
                ftl_type = get_ftl_type(d)
                plot_requestscale_count_imm_bw(d, benchname, save, ftl_type)

            } else if (testclass == 'scale-size-compare') {
                benchname = 'scale-size-compare'
                # save=T
                save=F
                plot_reqscale_size_bw_compare(d, benchname, save)

            } else if (testclass == 'scale-count-compare') {
                benchname = 'scale-count-compare'
                # save=T
                save=F
                plot_reqscale_count_bw_compare(d, benchname, save)

            } else if (testclass == 'alignment') {
                print(d)
                benchname = 'alignment'
                # save=T
                save=T
                ftl_type = get_ftl_type(d)
                plot_alignment_imm_bw(d, benchname, save, ftl_type)
                plot_alignment_total_wa(d, benchname, save, ftl_type)
                plot_alignment_gc_duration(d, benchname, save, ftl_type)
                plot_alignment_write_tax(d, benchname, save, ftl_type)

            } else if (testclass == 'grouping-in-space-compare' || testclass == 'grouping-in-time-compare') {
                benchname = testclass
                save=T
                # save=F
                plot_nkftl_reclaim_flash_space_wa_groupinspace(d, benchname, save, reclaim='flash')
                plot_nkftl_reclaim_flash_space_wa_groupinspace(d, benchname, save, reclaim='ram')
                plot_nkftl_grouping_write_tax(d, benchname, save, reclaim='flash')
                plot_nkftl_grouping_write_tax(d, benchname, save, reclaim='ram')
            }
            
            # -----------  locality read   ----------------------------
            # benchname='localityread-non-seg'
            # save=T
            # save=F
            # plot_locality_imm_read_bw(d, benchname, save)
            # plot_locality_imm_read_ra(d, benchname, save) 
            # plot_interest_miss_ratio(d, benchname, save)

            # -----------  locality write  ----------------------------
            benchname='localitywrite-non-seg'
            save=T
            # save=F
            # plot_locality_imm_write_bw(d, benchname, save)
            # plot_locality_imm_write_wa(d, benchname, save)
            # plot_locality_imm_write_ra(d, benchname, save)
            # plot_total_wa(d, benchname, save)
            # plot_interest_miss_ratio(d, benchname, save)
            plot_locality_write_tax(d, benchname, save)

            # plot_gc_overhead_ratio_locality(d, benchname, save)
            # plot_gc_overhead_ratio_locality(d, benchname, save)
            # plot_gc_duration(d, benchname, save)
            # plot_total_wa_locality(d, benchname, save)

            # -----------  group in time/space --------------------------
            # benchname='groupintime'
            # benchname='groupinspace-nonseg'
            # print(d)
            # benchname='groupinspace-seg'
            # save=T
            # save=F
            # plot_total_wa_groupintime(d, benchname, save)
            # plot_gc_overhead_ratio_groupintime(d, benchname, save)
            # plot_imm_write_bw_groupintime(d, benchname, save)
            # plot_grouping_write_tax(d, benchname, save)


        },

        get_ftl_type = function(d) 
        {
            t = unique(d$ftl_type)
            if (length(t) != 1) {
                stop("ftl type is not unique")
            }
            return(t[1])
        },

        add_max_rw_bw = function(d)
        {
            channel_r_bw = (d$page_size / MB) / (d$page_read_time / SEC)
            channel_w_bw = (d$page_size / MB) / (d$page_prog_time / SEC)
            channel_erase_bw = (d$block_size / MB) / (d$block_erase_time / SEC)
            d = transform(d, max_r_bw=channel_r_bw * n_channels_per_dev)
            d = transform(d, max_w_bw=channel_w_bw * n_channels_per_dev)
            d = transform(d, max_erase_bw=channel_erase_bw * n_channels_per_dev)

            return(d)
        },


        plot_reqscale_size_bw_compare = 
            function(d, benchname='nobenchname', save=FALSE, ftl_type="compare")
        {
            d = transform(d, bw=traffic_size/interest_wl_dur)

            d = ddply(d, .(operation), ddply_calc_bw_by_op)

            p = ggplot(d, aes(x=factor(chunk_size/KB), y=bw_util, fill=ftl_type)) +
                geom_bar(stat='identity', position='dodge') +
                # geom_point() +
                xlab('Chunk size (KB)') +
                ylab('Bandwidth Utilization') +
                scale_fill_grey() +
                # scale_y_continuous(limits=c(0, 1500)) +
                facet_grid(~operation) +
                ggplot_common_addon() +
                theme_zplot() +
                theme(legend.position='top', legend.title=element_blank())
            print(p) 
            save_plot(p, paste(ftl_type, benchname, 'size-imm-r-w-bw', sep='-'), save)
        },

        plot_reqscale_count_bw_compare = 
            function(d, benchname='nobenchname', save=FALSE, ftl_type="compare")
        {
            d = transform(d, bw=traffic_size/interest_wl_dur)

            d = ddply(d, .(operation), ddply_calc_bw_by_op)

            p = ggplot(d, aes(x=factor(ncq_depth), y=bw_util, fill=ftl_type)) +
                geom_bar(stat='identity', position='dodge') +
                xlab('Num of Concurrent Requests') +
                ylab('Bandwidth Utilization') +
                scale_fill_grey() +
                facet_grid(~operation) +
                ggplot_common_addon() +
                theme_zplot() +
                theme(legend.position='top', legend.title=element_blank())
            print(p) 
            save_plot(p, paste(ftl_type, benchname, 'count-imm-r-w-bw', sep=''), save)
        },


        # -----------------------------------------------------------
        #                     Alignment
        # -----------------------------------------------------------
        plot_alignment_imm_bw = function(d, benchname='nobenchname', save=FALSE, ftl_type="UNKNOWFTL")
        {
            d = transform(d, bw=traffic_size/interest_wl_dur)
            p = ggplot(d, aes(x=is_aligned, y=bw, fill=operation)) +
                geom_bar(stat='identity', position='dodge') +
                xlab('Aligned') +
                ylab('Bandwidth (MB/s)') +
                scale_fill_grey() +
                ggplot_common_addon() +
                theme_zplot() +
                theme(legend.position='top', legend.title=element_blank())
            print(p) 
            save_plot(p, paste(ftl_type, benchname, 'size-imm-r-w-bw', sep='-'), save)
        },
        plot_alignment_total_wa = function(d, benchname='nobenchname', save=FALSE, ftl_type="UKNOWN")
        {
            d = transform(d, wa=(interest_flash_write_bytes+gc_flash_write_bytes)/interest_fg_write_bytes)
            p = ggplot(d, aes(x=is_aligned, y=wa)) +
                geom_bar(stat='identity', position='dodge') +
                geom_text(aes(label=round(wa, 2)), vjust=1.1, color='white') +
                xlab('Is Aligned') +
                ylab('Write Amplification') +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            save_plot(p, paste(ftl_type, benchname, 'total-wa', sep=''), save)
        },
        plot_alignment_gc_duration = function(d, benchname='nobenchname', save=FALSE, ftl_type="UNKNOWFTL")
        {
            p = ggplot(d, aes(x=is_aligned, y=gc_duration)) +
                geom_bar(stat='identity', position='dodge') +
                xlab('Is Aligned') +
                ylab('GC time (sec)') +
                ggplot_common_addon() +
                theme_zplot()
            print(p) 
            save_plot(p, paste(ftl_type, benchname, 'gc-duration', sep=''), save)
        },
        plot_alignment_write_tax = function(d, benchname='nobenchname', save=FALSE, ftl_type="UNKNOWFTL")
        {
            # gc dur/foreground traffic 
            d = transform(d, write_tax = gc_duration / (traffic_size*MB/GB))
            p = ggplot(d, aes(x=is_aligned, y=write_tax)) +
                geom_bar(stat='identity') +
                xlab('Is Aligned') +
                ylab('Write Tax (Sec/GB)') +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            save_plot(p, paste(ftl_type, benchname, 'write-tax', sep=''), save)
        },


        # -----------------------------------------------------------
        #                     Request scale
        # -----------------------------------------------------------
        plot_requestscale_size_imm_bw = function(d, benchname='nobenchname', save=FALSE, ftl_type="UNKNOWFTL")
        {
            d = transform(d, bw=traffic_size/interest_wl_dur)
            p = ggplot(d, aes(x=factor(chunk_size/KB), y=bw, fill=operation)) +
                geom_bar(stat='identity', position='dodge') +
                xlab('Chunk size (KB)') +
                ylab('Bandwidth (MB/s)') +
                scale_fill_grey() +
                # scale_y_continuous(limits=c(0, 1500)) +
                ggplot_common_addon() +
                theme_zplot() +
                theme(legend.position='top', legend.title=element_blank())
            print(p) 
            save_plot(p, paste(ftl_type, benchname, 'size-imm-r-w-bw', sep='-'), save)
        },
        plot_requestscale_count_imm_bw = function(d, benchname='nobenchname', save=FALSE, ftl_type="UNKNOWFTL")
        {
            d = transform(d, bw=traffic_size/interest_wl_dur)
            p = ggplot(d, aes(x=factor(ncq_depth), y=bw, fill=operation)) +
                geom_bar(stat='identity', position='dodge') +
                xlab('Num of Writers') +
                ylab('Bandwidth (MB/s)') +
                scale_fill_grey() +
                scale_y_continuous(limits=c(0, 1500)) +
                ggplot_common_addon() +
                theme_zplot() +
                theme(legend.position='top', legend.title=element_blank())
            print(p) 
            save_plot(p, paste(ftl_type, benchname, 'count-imm-r-w-bw', sep=''), save)
        },

        # -----------------------------------------------------------
        #                     Locality
        # -----------------------------------------------------------
        plot_locality_write_tax = function(d, benchname='nobenchname', save=FALSE)
        {
            # gc dur/foreground traffic 
            d = cols_to_num(d, c('gc_duration', 'traffic_size'))
            d = transform(d, write_tax = gc_duration / (traffic_size*MB/GB))
            print(d)
            p = ggplot(d, aes(x=factor(space_size), y=write_tax)) +
                geom_bar(stat='identity') +
                xlab('Span (MB)') +
                ylab('Write Tax (Sec/GB)') +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            save_plot(p, paste(benchname, 'write-tax', sep=''), save)
        },
        plot_locality_imm_write_bw = function(d, benchname='nobenchname', save=FALSE)
        {
            d = transform(d, bw=traffic_size/interest_wl_dur)
            p = ggplot(d, aes(x=factor(space_size), y=bw)) +
                geom_bar(stat='identity') +
                xlab('Span of working set (MB)') +
                ylab('Bandwidth (MB/s)') +
                ggplot_common_addon() +
                theme_zplot()
            print(p) 
            save_plot(p, paste(benchname, 'imm-write-bw', sep=''), save)
        },
        plot_locality_imm_read_bw = function(d, benchname='nobenchname', save=FALSE)
        {
            # t = "space_size bw
                 # 1024       466.2344
                 # 128        942.8403"
            # d = read.table(text=t, header=T)
            d = transform(d, bw=traffic_size/interest_wl_dur)
            p = ggplot(d, aes(x=factor(space_size), y=bw)) +
                geom_bar(stat='identity') +
                xlab('Span of working set (MB)') +
                ylab('Bandwidth (MB/s)') +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            save_plot(p, paste(benchname, 'imm-read-bw', sep=''), save)
        },
        plot_gc_duration = function(d, benchname='nobenchname', save=FALSE)
        {
            p = ggplot(d, aes(x=factor(space_size), y=gc_duration)) +
                geom_bar(stat='identity', position='dodge') +
                xlab('Span of working set (MB)') +
                ylab('GC time (sec)') +
                ggplot_common_addon() +
                theme_zplot()
            print(p) 
            save_plot(p, paste(benchname, 'gc-duration', sep=''), save)
        },
        plot_locality_imm_read_ra = function(d, benchname='nobenchname', save=FALSE)
        {
            # flash read / foreground traffic size (read or write)
            d = transform(d, interest_ra = interest_flash_read_bytes / interest_fg_read_bytes)
            p = ggplot(d, aes(x=factor(space_size), y=interest_ra)) +
                geom_bar(stat='identity') +
                xlab('Span of working set (MB)') +
                ylab('Read Amplification') +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            save_plot(p, paste(benchname, 'imm-read-wa', sep=''), save)
        },
        plot_locality_imm_write_wa = function(d, benchname='nobenchname', save=FALSE)
        {
            # flash read / foreground traffic size (read or write)
            d = transform(d, interest_wa=interest_flash_write_bytes / interest_fg_write_bytes)
            p = ggplot(d, aes(x=factor(space_size), y=interest_wa)) +
                geom_bar(stat='identity', position='dodge') +
                geom_text(aes(label=round(interest_wa, 2)), vjust=1.1, color='white') +
                xlab('Span of working set (MB)') +
                ylab('Immediate Write Amplification') +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            save_plot(p, paste(benchname, 'imm-write-wa', sep=''), save)
        },
        plot_locality_imm_write_ra = function(d, benchname='nobenchname', save=FALSE)
        {
            d = transform(d, interest_ra=interest_flash_read_bytes / interest_fg_write_bytes)
            p = ggplot(d, aes(x=factor(space_size), y=interest_ra)) +
                geom_bar(stat='identity', position='dodge') +
                geom_text(aes(label=round(interest_ra, 3)), vjust=-0.1, color='black') +
                xlab('Span of working set (MB)') +
                ylab('Immediate Read Amplification') +
                scale_y_continuous(limits=c(0, 1)) +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            save_plot(p, paste(benchname, 'imm-write-ra', sep=''), save)
        },
        plot_total_wa = function(d, benchname='nobenchname', save=FALSE)
        {
            d = transform(d, wa=(interest_flash_write_bytes+gc_flash_write_bytes)/interest_fg_write_bytes)
            p = ggplot(d, aes(x=factor(space_size), y=wa)) +
                geom_bar(stat='identity', position='dodge') +
                geom_text(aes(label=round(wa, 2)), vjust=1.1, color='white') +
                xlab('Span of working set (MB)') +
                ylab('Write Amplification') +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            save_plot(p, paste(benchname, 'total-wa', sep=''), save)
        },
        plot_interest_miss_ratio = function(d, benchname='nobenchname', save=FALSE)
        {
            d = transform(d, interest_missratio = interest_miss_cnt / (interest_hit_cnt + interest_miss_cnt))
            p = ggplot(d, aes(x=factor(space_size), y=interest_missratio)) +
                geom_bar(stat='identity', position='dodge') +
                xlab('Span of working set (MB)') +
                ylab('Miss Ratio') +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            save_plot(p, paste(benchname, 'interest-miss-ratio', sep=''), save)
        },
        plot_gc_overhead_ratio_locality = function(d, benchname='nobenchname', save=FALSE)
        {
            d = transform(d, overhead_ratio=gc_duration/interest_wl_dur)
            p = ggplot(d, aes(x=factor(space_size), y=overhead_ratio)) +
                geom_bar(stat='identity', position='dodge') +
                xlab('Span of working set') +
                ylab('GC Overhead Ratio') +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            save_plot(p, paste(benchname, 'gc-overhead-ratio', sep=''), save)
        },
        save_plot = function(p, filename, save, h=3, w=3)
        {
            print(filename)
            filename = paste(filename, '.pdf', sep='')
            if (save == TRUE) {
                ggsave(filename, plot=p, height=h, width=w) 
            }
        },

        #
        # ====================== Group In Time ======================
        #
        plot_grouping_write_tax = function(d, benchname='nobenchname', save=FALSE)
        {
            # gc dur/foreground traffic 
            d = transform(d, write_tax = gc_duration / (traffic_size*MB/GB))
            p = ggplot(d, aes(x=grouping, y=write_tax)) +
                geom_bar(stat='identity') +
                xlab('Grouping') +
                ylab('Write Tax (Sec/GB)') +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            save_plot(p, paste(benchname, 'write-tax', sep=''), save)
        },
        plot_imm_write_bw_groupintime = function(d, benchname, save)
        {
            d = transform(d, bw=2*traffic_size/interest_wl_dur)
            p = ggplot(d, aes(x=grouping, y=bw)) +
                geom_bar(stat='identity') +
                xlab('Grouping') +
                ylab('Bandwidth (MB/s)') +
                ggplot_common_addon() +
                theme_zplot()
            print(p) 

            if (save == TRUE) {
                ggsave(paste(benchname, '-imm-write-bw.pdf', sep=''), plot=p, h=3, w=3) 
            }
        },
        plot_total_wa_groupintime = function(d, benchname, save)
        {
            d = transform(d, wa=(interest_flash_write_bytes+gc_flash_write_bytes)/interest_fg_write_bytes)
            p = ggplot(d, aes(x=grouping, y=wa)) +
                geom_bar(stat='identity', position='dodge') +
                xlab('Grouping') +
                ylab('Write Amplification') +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            if (save == TRUE) {
                ggsave(paste(benchname, '-total-wa.pdf', sep=''), plot=p, h=3, w=3) 
            }
        },
        plot_gc_overhead_ratio_groupintime = function(d, benchname, save)
        {
            d = transform(d, overhead_ratio=gc_duration/interest_wl_dur)
            p = ggplot(d, aes(x=grouping, y=overhead_ratio)) +
                geom_bar(stat='identity', position='dodge') +
                xlab('Grouping') +
                ylab('GC Overhead Ratio') +
                ggplot_common_addon() +
                theme_zplot()
            print(p)
            if (save == TRUE) {
                ggsave(paste(benchname, '-gc-overhead-ratio.pdf', sep=''), plot=p, h=3, w=3) 
            }
        },

        #
        # ====================== Group In Space ======================
        #
        calc_phased_gc = function(d)
        {
            d = transform(d, nonmerge_works = as.numeric(nonmerge_gc_flash_erase_bytes > 0))
            d = transform(d, flash_space_relaim_gc_dur = 
                      nonmerge_gc_duration * nonmerge_works + gc_duration * (1 - nonmerge_works))
            d = transform(d, flash_space_relaim_gc_write_bytes = 
                      nonmerge_gc_flash_write_bytes * nonmerge_works 
                      + gc_flash_write_bytes * (1 - nonmerge_works))

            d = transform(d, ram_space_relaim_gc_dur = nonmerge_gc_duration + gc_duration)
            d = transform(d, ram_space_relaim_gc_write_bytes = nonmerge_gc_flash_write_bytes + 
                          gc_flash_write_bytes)

            return(d)
        },
        plot_nkftl_reclaim_flash_space_wa_groupinspace = function(d, benchname, save, reclaim)
        {
            d = calc_phased_gc(d)

            if (reclaim == 'flash') {
                d = transform(d, wa=(interest_flash_write_bytes+flash_space_relaim_gc_write_bytes)/interest_fg_write_bytes)
            } else if (reclaim == 'ram') {
                d = transform(d, wa=(interest_flash_write_bytes+ram_space_relaim_gc_write_bytes)/interest_fg_write_bytes)
            }
            p = ggplot(d, aes(x=grouping, y=wa, fill=ftl_type)) +
                geom_bar(stat='identity', position='dodge') +
                xlab('Grouping') +
                ylab('Write Amplification') +
                scale_fill_grey() +
                ggplot_common_addon() +
                theme_zplot() +
                ggtitle(paste('To reclaim', reclaim)) +
                theme(legend.position='top', legend.title=element_blank())
            print(p)
            if (save == TRUE) {
                save_plot(p, paste(benchname, reclaim, 'total-wa', sep='-'), save)
            }
        },
 
        plot_nkftl_grouping_write_tax = function(d, benchname='nobenchname', save=FALSE, reclaim='ram')
        {
            d = calc_phased_gc(d)

            d = transform(d, traffic_gb = traffic_size*MB/GB)

            if (reclaim == 'flash') {
                d = transform(d, write_tax=flash_space_relaim_gc_dur/traffic_gb)
            } else if (reclaim == 'ram') {
                d = transform(d, write_tax=ram_space_relaim_gc_dur/traffic_gb)
            }
            print(d)
 
            p = ggplot(d, aes(x=grouping, y=write_tax, fill=ftl_type)) +
                geom_bar(stat='identity', position='dodge') +
                xlab('Grouping') +
                ylab('Write Tax (Sec/GB)') +
                scale_fill_grey() +
                ggplot_common_addon() +
                theme_zplot() +
                ggtitle(paste('To reclaim', reclaim)) +
                theme(legend.position='top', legend.title=element_blank())
            print(p)
            save_plot(p, paste(benchname, reclaim, 'write-tax', sep='-'), save)
        },
 
        phony = function() {}
    )
)

AlignmentBench <- setRefClass("AlignmentBench",
    fields = list(expname='character'),
    methods = list(
        run = function(exp_rel_path, testclass='explore')
        {
            d = aggregate_subexp_list_to_dataframe(exp_rel_path, 
                                               SubExpAlignment)

            d = cols_to_num(d, c('merge.partial_merge', 
                                 'merge.switch_merge',
                                 'merge.full_merge',
                                 'pagesize',
                                 'segment_bytes',
                                 'flash_w_bytes',
                                 'flash_r_bytes',
                                 'flash_e_bytes',
                                 'write_bytes',
                                 'read_bytes',
                'fullmerge_ops.physical_read', 'fullmerge_ops.physical_write', 
                'fullmerge_ops.phy_block_erase', 'switchmerge_ops.physical_read', 
                'switchmerge_ops.physical_write', 'switchmerge_ops.phy_block_erase',
                'partialmerge_ops.physical_read', 'partialmerge_ops.physical_write',
                'partialmerge_ops.phy_block_erase', 'simpleerase_ops.physical_read',
                'simpleerase_ops.physical_write', 'simpleerase_ops.phy_block_erase'
                                 ))

            if (testclass == 'explore') {
                plot_traffic(d)
                plot_merges(d)
                plot_moves(d)
                plot_ftl_wa(d)
            } else if (testclass == 'paper') {
                # plot_traffic_paper(d)
                # plot_unaligned_vs_filesize(d)
                plot_unaligned_size(d)
                # plot_agg_file_size(d)
            }
        },
        plot_unaligned_size = function(d)
        {
            # d = subset(d, segment_bytes == 128*KB)
            d = transform(d, segment_bytes=to_human_factor(segment_bytes))
            d = transform(d, Unaligned=fullmerge_ops.physical_write + 
                    partialmerge_ops.physical_write)
            d = transform(d, Unaligned = Unaligned*pagesize/MB)
            d = transform(d, benchname=revalue(benchname,
                c('Sqlite-random'='Sqlite-rand', 'Sqlite-sequential'='Sqlite-seq')))

            p = ggplot(d, aes(x=filesystem, y=Unaligned)) +
                geom_bar(stat='identity', position='dodge') +
                facet_grid(segment_bytes~benchname) +
                theme_zplot() +
                scale_fill_grey() +
                ylab('Unaligned Data Size (MB)') +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                ggtitle('Unaligned Size')

            print(p)
            save_plot(p, 'unaligned-ratio', h=3.4, w=4.6, save=T)
        },
        plot_agg_file_size = function(d)
        {
            d = subset(d, segment_bytes == 128*KB)
            d = transform(d, segment_bytes=to_human_factor(segment_bytes))
            d = transform(d, Unaligned=fullmerge_ops.physical_write + 
                    partialmerge_ops.physical_write)
            d = transform(d, Unaligned = Unaligned*pagesize/MB)
            d = transform(d, benchname=revalue(benchname,
                c('Sqlite-random'='Sqlite-rand', 'Sqlite-sequential'='Sqlite-seq')))
            d$agg_file_size = as.numeric(as.character(revalue(d$benchname, 
                c('LevelDB'=672, 'Sqlite-rand'=19, 
                  'Sqlite-seq'=19))))

            dup = duplicated(d$benchname)
            d = d[!dup, ]

            p = ggplot(d, aes(x=benchname,y=agg_file_size)) +
                geom_bar(stat='identity', position='dodge') +
                geom_text(aes(label=round(agg_file_size, 1)), 
                          vjust=0,
                          position=position_dodge(1)) +
                theme_zplot() +
                scale_fill_grey() +
                ylab('Aggregated File Size') +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            print(p)
        },
        plot_unaligned_vs_filesize = function(d)
        {
            d = subset(d, segment_bytes == 128*KB)
            d = transform(d, segment_bytes=to_human_factor(segment_bytes))
            d = transform(d, Unaligned=fullmerge_ops.physical_write + 
                    partialmerge_ops.physical_write)
            d = transform(d, Unaligned = Unaligned*pagesize/MB)
            d = transform(d, benchname=revalue(benchname,
                c('Sqlite-random'='Sqlite-rand', 'Sqlite-sequential'='Sqlite-seq')))
            d$agg_file_size = as.numeric(as.character(revalue(d$benchname, 
                c('LevelDB'=672, 'Sqlite-rand'=19, 
                  'Sqlite-seq'=19))))

            d = transform(d, unaligned_ratio=Unaligned/agg_file_size)

            d = subset(d, benchname != 'Varmail')
            print(d)

            p = ggplot(d, aes(x=filesystem, y=unaligned_ratio)) +
                geom_bar(stat='identity', position='dodge') +
                geom_text(aes(label=round(unaligned_ratio, 1)), 
                          vjust=0,
                          position=position_dodge(1)) +
                facet_grid(~benchname) +
                theme_zplot() +
                scale_fill_grey() +
                ylab('Unaligned Ratio') +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                ggtitle('Unaligned Size/File Size')

            print(p)
            save_plot(p, 'unaligned-ratio', h=3.4, w=4.6, save=T)
        },
        plot_moves = function(d)
        {
            d = transform(d, segment_bytes=to_human_factor(segment_bytes))
            d = melt(d, id=c('filesystem', 'segment_bytes', 'ncq_depth', 'pagesize',
                             'benchname'), 
                     measure=c(
                'fullmerge_ops.physical_write', 
                # 'fullmerge_ops.phy_block_erase', 
                # 'switchmerge_ops.physical_write', 
                # 'switchmerge_ops.phy_block_erase',
                'partialmerge_ops.physical_write'
                # 'partialmerge_ops.phy_block_erase', 
                # 'simpleerase_ops.physical_write', 
                # 'simpleerase_ops.phy_block_erase'
                ))
            d = plyr::rename(d, c('variable'='move_type',
                            'value'='move_count'))
            d = transform(d, moved_size = move_count*pagesize/MB)
            p = ggplot(d, aes(x=filesystem, y=moved_size, fill=move_type)) +
                geom_bar(aes(order=desc(move_type)), stat='identity', position='dodge') +
                geom_text(aes(label=round(moved_size)), position=position_dodge(1))+
                facet_grid(benchname~segment_bytes)

            print(p)
        },
        plot_merges = function(d)
        {
            d = transform(d, segment_bytes=to_human_factor(segment_bytes))
            d = melt(d, id=c('filesystem', 'segment_bytes', 'ncq_depth',
                             'benchname'), 
                     measure=c('merge.switch_merge', 'merge.full_merge', 
                               'merge.partial_merge'))
            d = plyr::rename(d, c('variable'='merge_type',
                            'value'='merge_count'))
            p = ggplot(d, aes(x=filesystem, y=merge_count, fill=merge_type)) +
                geom_bar(aes(order=desc(merge_type)), stat='identity', position='dodge') +
                facet_grid(benchname~segment_bytes)
            print(p)
        },
        plot_traffic = function(d)
        {
            d = transform(d, segment_bytes=to_human_factor(segment_bytes))
            d = melt(d, id=c('filesystem', 'segment_bytes', 'ncq_depth', 'benchname'), 
                     measure=c('write_bytes', 'read_bytes', 'discarded_bytes'))
            d = plyr::rename(d, c('variable'='operation',
                            'value'='size'))
            d = transform(d, size = as.numeric(size)/MB)
            print(d)
            p = ggplot(d, aes(x=filesystem, y=size, fill=operation)) +
                geom_bar(aes(order=desc(operation)), stat='identity', position='dodge') +
                facet_grid(benchname~segment_bytes) +
                ggtitle('Traffic')
            print(p)
        },
        plot_ftl_wa = function(d)
        {
            # flash writes / ext4-nj
            d = transform(d, segment_bytes=to_human_factor(segment_bytes))
            d = transform(d, WAR=flash_w_bytes/write_bytes)
            p = ggplot(d, aes(x=filesystem, y=WAR)) +
                geom_bar(stat='identity', position='dodge') +
                geom_hline(y=1) +
                scale_y_continuous(breaks=seq(1, 2, 0.2)) +
                facet_grid(benchname~segment_bytes) +
                theme_zplot() +
                ggtitle('FTL Write Amp Ratio') +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                theme(panel.margin.x = unit(1, "lines")) 
            print(p)
        },


        plot_traffic_paper = function(d)
        {
            d = melt(d, id=c('filesystem', 'segment_bytes', 'ncq_depth', 'benchname'), 
                     measure=c('write_bytes', 'read_bytes', 'discarded_bytes'))
            d = plyr::rename(d, c('variable'='operation',
                            'value'='size'))
            d$operation = revalue(d$operation, c('write_bytes'='write', 
                                               'read_bytes'='read',
                                               'discarded_bytes'='discard'))
            d = subset(d, operation %in% c('write', 'discard'))

            d = transform(d, size = as.numeric(size)/MB)
            
            d = subset(d, segment_bytes == 128*KB)

            d = transform(d, benchname=revalue(benchname,
                c('Sqlite-random'='Sqlite-rand', 'Sqlite-sequential'='Sqlite-seq')))

            p = ggplot(d, aes(x=filesystem, y=size, fill=operation)) +
                geom_bar(aes(order=desc(operation)), stat='identity', position='dodge') +
                facet_grid(~benchname) +
                scale_y_continuous(breaks=seq(0, 1024*10, 1024),
                                   labels=seq(0, 1024*10, 1024)/1024) +
                scale_fill_grey() +
                xlab('file system') +
                ylab('size (GB)') + 
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                theme(panel.margin.x = unit(1, "lines")) +
                theme(legend.position='top', legend.title=element_blank())
            print(p)
            save_plot(p, paste('app-traffic', sep=''), save=T, h=3.2, w=5)
        },

        save_plot = function(p, filename, save, h=3, w=3)
        {
            print(filename)
            filename = paste(filename, '.pdf', sep='')
            if (save == TRUE) {
                ggsave(filename, plot=p, height=h, width=w) 
            }
        }

    )
)

SemanticsBench <- setRefClass("SemanticsBench",
    fields = list(expname='character'),
    methods = list(
        run = function(exp_rel_path, testclass='explore')
        {
            d = aggregate_subexp_list_to_dataframe2(exp_rel_path, 
                                               SubExpSemantics)
            d = cols_to_num(d, c('segment_bytes'))
            plot_semantic_sizes(d)
        },
        plot_semantic_sizes = function(d)
        {
            d$blocksize = NULL
            d$pagesize = NULL
            d = melt(d, id=c('filesystem', 'benchname', 'segment_bytes'))
            d = plyr::rename(d, c('variable'='datatype', 'value'='size'))
            d = cols_to_num(d, c('size'))
            print(d)
            p = ggplot(d, aes(x=filesystem, y=size/MB, fill=datatype)) +
                geom_bar(stat='identity', position='dodge') +
                scale_fill_grey() +
                ylab('Size (MB)') +
                facet_grid(segment_bytes~benchname) +
                theme_zplot()
            print(p)
        }
    )
)



GcLogBenchNkftl <- setRefClass("GcLogBench",
    fields = list(),
    methods = list(
        run = function(exp_rel_path)
        {
            subexpiter = SubExpIter(exp_rel_path=exp_rel_path)
            result = subexpiter$iter_each_subexp(SubExpGcLogParsedNkftl)
        }
    )
)

FileSnakeBench <- setRefClass("FileSnakeBench",
    fields = list(expname='character'),
    methods = list(
        run = function(exp_rel_path, testclass)
        {
            d = aggregate_subexp_list_to_dataframe(exp_rel_path, 
                                               SubExpFileSnake)
            d = cols_to_num(d, c('file_size', 'fullmerge_ops.physical_write', 'pagesize'))

            print(d)
            # plot_unaligned(d, save=T)
            plot_unaligned_by_write_pre_file(d, save=T)
        },
        calc_unaligned_data = function(d)
        {
            d = transform(d, unaligned=fullmerge_ops.physical_write * pagesize)
            return(d)
        },
        plot_unaligned = function(d, save)
        {
            d = calc_unaligned_data(d)
            d = transform(d, file_size = to_human_factor(file_size))
            d = transform(d, unaligned = unaligned/MB)
            p = ggplot(d, aes(x=filesystem, y=unaligned, fill=file_size)) +
                geom_bar(stat='identity', position='dodge') +
                ylab('Unaligned Data (MB)') +
                xlab('File system') +
                scale_fill_grey() +
                theme_zplot() +
                theme(legend.position='top', legend.title=element_blank())
            print(p)
            save_plot(p, 'filesnake' , save=T)
        },
        plot_unaligned_by_write_pre_file = function(d, save)
        {
            d = calc_unaligned_data(d)
            d = transform(d, file_size = to_human_factor(file_size))
            d = transform(d, unaligned = unaligned/MB)
            p = ggplot(d, aes(x=filesystem, y=unaligned, fill=write_pre_file)) +
                geom_bar(stat='identity', position='dodge') +
                ylab('Unaligned Data (MB)') +
                xlab('File system') +
                scale_fill_grey() +
                theme_zplot() +
                theme(legend.position='top', legend.title=element_blank())
            print(p)
            save_plot(p, 'filesnake-with-prefile' , save=T)
        }

    )
)

c_______main_functions__________ <- function(){}


sample_run <- function()
{
    subexpiter = SubExpIter(exp_rel_path='parallelgc002')
    subexpiter$iter_each_subexp(SubExpTimelineAnalyzer)
}

main <- function()
{
    eventsbench = BlkParseEventsBench()
    # eventsbench$run('leveldb-s')


    localitybench = LocalityBench()
    # localitybench$run('fullscalelocalitywithgc16gcthreads')
    # localitybench$run(c('1tbspace16gcthreads', 
                        # 'fullscalelocalitywithgc16gcthreads'))

    # localitybench$run(c('fullscalelocalitywithgc16gcthreads', 
                        # '4-2-512-256mb-16gcthreads')) # <<<<< GOOD ONE


    # GOOD ONES

    # request scale
    # localitybench$run('requestscalesize') # size
    # localitybench$run('requestcount002') # count

    # Locality Read
    # localitybench$run('localityreadforWA', 'non') # <<<< GOOD ONE, for paper

    # Locality Write
    # localitybench$run('localitywriteforWA', 'non') # PAPER: write locality

    # Grouping in Time
    # localitybench$run('groupingintime64traffic') # PAPER

    # Grouping In Space
    # localitybench$run('groupinginspace64traffic') # non-segmented ftl
    # localitybench$run('segmentedgroupinginspae') # segmented ftl


    # localitybench$run('testwara')
    # localitybench$run('testhitmiss')
    # localitybench$run('groupintimeremovecurbeforegc')
    # localitybench$run('scalesize')

    ############ NKFTL
    # localitybench$run('officialnkftlreqcount', 'scale-count')
    # localitybench$run('officialnkftlreqsize', 'scale-size')
    # localitybench$run('limitedgcthreads', 'alignment')

    ############ Compare DFTL and NKFTL
    # localitybench$run(c('requestscalesize', 'officialnkftlreqsize'),
                      # 'scale-size-compare')
    # localitybench$run(c('requestcount002', 'realonlyonedatagroupreqcount'),
                      # 'scale-count-compare') # both are non-seg
    # localitybench$run(c('nkftl2128threadsgo', 'dftldes128threadgogogo'), 'groupinspace-compare')
    # localitybench$run(c('nkftl888', '888'), 'grouping-in-time-compare')
    # localitybench$run(c('bothftlgroupingintime'), 'grouping-in-space-compare')




    # localitybench$run('groupinspacewithnonmergegc', 'groupinspace-nkftl')
    # localitybench$run(c(
                        # # 'groupinspacewithnonmergegc'),
                        # 'segmentedgroupinginspae'), 
                      # 'groupinspace-nkftl')

    # localitybench$run(c('dftl64threads', 'nkftl64threadshaha'),
                      # 'groupinspace-compare')
    # localitybench$run(c('shouldbegood', 'groupinspacedeftlshouldbegood'), 
                      # 'grouping-in-space-compare')

    alignmentbench = AlignmentBench()
    # alignmentbench$run(c('shouldbeperfect', 'moresegsizes'))
    # alignmentbench$run(c('shouldbeperfect', 'moresegsizes', 'ssdncq1'))
    # alignmentbench$run(c('f2fssolo')) 
    # alignmentbench$run(c('newallocplea')) 
    # alignmentbench$run(c('f2fsgcwithnewalloc')) 
    # alignmentbench$run(c('mysqlnewallocdiscard')) 
    # alignmentbench$run(c('ssdncq1'))          # LEVELDB
    # alignmentbench$run(c('allfsallseg'))      # sqlbench test-insert-rand
    # alignmentbench$run(c('sqlbenchseqfsseg')) # sqlbench test-insert-seq

    # alignmentbench$run(c('largerscalesqlbench')) 
    # alignmentbench$run(c('ext5journaloff')) 
    # alignmentbench$run(c('halfandhalf')) 
    # alignmentbench$run(c('leveldb-for-reproduce')) 

    # alignmentbench$run(c('largeleveldb-fses', 'ext4nj-leveldblarge')) 
    # alignmentbench$run(c('testleveldbsize'))  
    # alignmentbench$run(c('testsize-3m'))  

    # good exploration
    # alignmentbench$run(c('largeleveldb-fses', 'ext4nj-leveldblarge')) # LEVELDB
    # alignmentbench$run(c('largerscalesqlbench', 'sqlbench-rand-ext4nj')) # MYSQL

    # For paper
    # alignmentbench$run(c('largeleveldb-fses', 'ext4nj-leveldblarge'), 
                       # testclass='paper') 

    # it has ext4, ext4-nj, f2fs
    # leveldb-random
    # segment 128KB, 1MB
    # 3 millions overwrites, max key 3 millions
    # overwrite
    # alignmentbench$run(c('leveldb-3m-2fs'))  

    # it has ext4, ext4-nj, f2fs
    # leveldb-random
    # segment 128KB, 1MB
    # 3 millions overwrites, max key 3 millions,
    # fillseq
    # alignmentbench$run(c('leveldb-fillseq-2m'))  

    # alignmentbench$run(c('leveldb-3m-vs-3000'))  
    # alignmentbench$run(c('leveldb-6m-mixed'))  # <<<<<<<< My choice
    # alignmentbench$run(c('leveldb-3m-vs-0.3m'))
    # alignmentbench$run(c('6m-overwrite-2fs'))
    # alignmentbench$run('leveldb-f2fs-gcoff')
    # alignmentbench$run('skewed-2fs')
    # alignmentbench$run('varmail-10s')
    # alignmentbench$run('sqlite-60000')
    # alignmentbench$run('sqlite-1gplus-traffic') # <<<<<<< Sqlite rand
    # alignmentbench$run('sqlite-1gplus-traffic-seq')  # <<<<<< Sqlite seq
    # alignmentbench$run('sqlite-f2fs-sleep')
    # alignmentbench$run('filebench8000')
    # alignmentbench$run('varmail-20s-2fs')

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # alignmentbench$run(c('leveldb-6m-mixed',
                         # 'sqlite-1gplus-traffic',
                         # 'sqlite-1gplus-traffic-seq',
                         # 'varmail-20s-2fs'
                         # ), testclass='paper')


    gclognkftl = GcLogBenchNkftl()
    # gclognkftl$run('leveldb-for-reproduce')
    # gclognkftl$run('leveldb-strace')
    # gclognkftl$run('testpadding4')
    # gclognkftl$run('leveldb-s')
    # gclognkftl$run('sqlite001')
    # gclognkftl$run('sqlite10000insertions')
    # gclognkftl$run('120KB500files')
    # gclognkftl$run('128KB500files')
    # gclognkftl$run('testleveldbsize')
    # gclognkftl$run(c('100mbactive'))
    # gclognkftl$run(c('leveldb-3m-2fs/subexp-4988791105346456006-f2fs-08-14-21-16-10-6780131142514500642'))
    # gclognkftl$run(c('leveldb-3m-2fs/subexp--9113365843140004544-ext4-08-14-21-19-17-4388540726801654847'))
    # gclognkftl$run(c('leveldb-6m-mixed/subexp-975211625979296731-f2fs-08-14-22-14-27-5741709161364677239'))
    # gclognkftl$run('6m-overwrite-2fs')
    # gclognkftl$run('leveldb-f2fs-gcoff')
    # gclognkftl$run('6m-overwrite-2fs')
    # gclognkftl$run('varmail-10s')
    # gclognkftl$run('filebench8000')
    # gclognkftl$run('sqlite-60000')
    # gclognkftl$run('sqlite-1gplus-traffic/subexp--7465453720427028283-f2fs-08-15-11-46-36-6164489463159846910')
    # gclognkftl$run(c('leveldb-6m-mixed'))





    # plot_space_vs_time('sqlite-1gplus-traffic/subexp--7465453720427028283-f2fs-08-15-11-46-36-6164489463159846910')
    # plot_space_vs_time('sqlite-f2fs-sleep')
    # plot_space_vs_time('varmail-20s-2fs/subexp--6804779358481257494-ext4-08-15-16-00-34--75703994369609570')
    # plot_space_vs_time('leveldb-grouping-ext4-f2fs/subexp--726201181520563967-ext4-08-21-09-03-11-6643118727579031895')
    # plot_space_vs_time('leveldb-grouping-ext4-f2fs/subexp-62556292754797433-ext4-08-21-08-52-34--6270999007285636801')
    # plot_space_vs_time('leveldb-grouping-ext4-f2fs/subexp-4391525182383652983-f2fs-08-21-08-58-03--4091120345161694806')
    # plot_space_vs_time('leveldb-add-btrfs-and-xfs/subexp--6447001191901936862-xfs-08-25-15-07-20--4308844762560320345')
    # plot_space_vs_time('sqlite-add-btrfs-xfs/subexp-3324937979580796683-btrfs-08-25-14-40-44-9178682186777245111')
    plot_space_vs_time('sqlite-btrfs-nomoutopt')
                       

    semantics = SemanticsBench()
    # semantics$run('leveldb-6m-mixed')
    # semantics$run('sqlite-1gplus-traffic')
    

    filesnake = FileSnakeBench()
    # filesnake$run('filesnake-bench')
    # filesnake$run('fadingsnake-big')
    # filesnake$run('fadingsnake-2fs')
    # filesnake$run('fadingsnake-with-pre')


    # gclognkftl$run('only1cputhere')

    print_global_plots()
}

main()
# test_main()



