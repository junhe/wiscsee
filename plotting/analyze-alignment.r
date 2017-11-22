# libraries
library(ggplot2)
library(plyr)
library(dplyr)
library(reshape2)
library(gridExtra)
library(jsonlite)
library(digest)

source('doraemon/header.r')
source('doraemon/organizers.r')
source('doraemon/file_parsers.r')

# copy the following so you can do sme()
sme <- function()
{
    WORKDIRECTORY= "/Users/junhe/workdir/analysis-script/"
    THISFILE     ='doraemon/analyze-alignment.r'
    setwd(WORKDIRECTORY)
    source(THISFILE)
}


c_______Unaligned_Size____________ <- function(){}

SubExpAlignment <- setRefClass("SubExpNkftlStats",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            confjson = create_configjson(subexppath=subexppath)
            recjson = RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))
            statsjson = StatsJson(
                  filepath=paste(subexppath, 'stats.json', sep='/'))

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
                flash_e_bytes=recjson$get_flash_ops_erase() * blocksize,
                disk_used_bytes=statsjson$get_disk_used_bytes()
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

AligmentPlotBase <- setRefClass("AligmentPlotBase",
    fields = c('expname', 'df'),
    methods = list(


        get_data = function(exp_rel_path)
        {
            d = aggregate_subexp_list_to_dataframe(exp_rel_path, 
                                               SubExpAlignment)
            d = clean_data(d)
            return(d)
        },
        filter_data_for_plot = function(d)
        {
            d = subset(d, filesystem != 'ext4-nj')
            d = subset(d, benchname != 'Sqlite-sequential')
            d = transform(d, benchname=revalue(benchname, c('Sqlite-random'='Sqlite')))
            return(d)
        },
        clean_data = function(d)
        {
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
            return(d)
        }
    )
)


TrafficSizePlot <- setRefClass("TrafficSizePlot",
    contains = c('AligmentPlotBase'),
    fields = c('expname', 'df'),
    methods = list(
        plot_traffic = function(exp_rel_path)
        {
            d = get_data(exp_rel_path)
            d = filter_data_for_plot(d)
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
            d = subset(d, filesystem != 'btrfs')

            p = ggplot(d, aes(x=filesystem, y=size, fill=operation)) +
                geom_bar(aes(order=desc(operation)), stat='identity', position='dodge') +
                facet_grid(~benchname) +
                scale_y_continuous(breaks=seq(0, 1024*10, 1*1024),
                                   labels=seq(0, 1024*10, 1*1024)/1024) +
                scale_fill_grey_dark() +
                xlab('') +
                ylab('Size (GB)') + 
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                theme(panel.margin.x = unit(1, "lines")) +
                theme(legend.position='top', legend.title=element_blank())
            print(p)
            save_plot(p, paste('app-traffic', sep=''), save=T, w=3.2, h=2.1)
        }
    )
)
 

UnalignedSizePlot <- setRefClass("UnalignedSizePlot",
    contains = c('AligmentPlotBase'),
    fields = c('expname', 'df'),
    methods = list(
        plot_unaligned_size = function(exp_rel_path)
        {
            d = get_data(exp_rel_path)
            d = filter_data_for_plot(d)
            d = transform(d, segment_bytes=to_human_factor(segment_bytes))
            d = transform(d, 
                Unaligned=fullmerge_ops.physical_write + partialmerge_ops.physical_write)
            d = transform(d, Unaligned = Unaligned*pagesize/MB)

            d = subset(d, filesystem != 'btrfs')


            d = transform(d, disk_used_bytes = as.numeric(as.character(disk_used_bytes)) / MB)
            dd = d[, c('filesystem', 'Unaligned', 'benchname', 'segment_bytes', 'disk_used_bytes')]
            dd = arrange(dd, benchname, filesystem, segment_bytes)
            print(dd)

            p = ggplot(d, aes(x=filesystem, y=Unaligned)) +
                geom_bar(stat='identity', position='dodge', width=0.8) +
                # geom_text(aes(label=round(Unaligned)), position=position_dodge(1), 
                          # color='black', hjust=0, vjust=0.5, angle=90) +
                facet_grid(segment_bytes~benchname) +
                theme_zplot() +
                scale_fill_grey_dark() +
                ylab('Unaligned Data Size (MB)') +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))

            print(p)
            save_plot(p, 'unaligned-size', w=3.2, h=2.1, save=T)
        }
    )
)
         

UnalignedRatioPlot_PAPER <- setRefClass("UnalignedRatioPlot_PAPER",
    contains = c('AligmentPlotBase'),
    fields = c('expname', 'df'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = get_data(exp_rel_path)
            d = filter_data_for_plot(d)
            d = cols_to_num(d, c('disk_used_bytes'))
            d = transform(d, segment_bytes=to_human_factor(segment_bytes))
            d = transform(d, 
                Unaligned=fullmerge_ops.physical_write + partialmerge_ops.physical_write)
            d = transform(d, Unaligned = Unaligned*pagesize)
            d = transform(d, unaligned_ratio = Unaligned / disk_used_bytes)

            d = subset(d, filesystem != 'btrfs')

            d = transform(d, limited_unaligned_ratio = 2.8)
            d$limited_unaligned_ratio = apply(d[,c('limited_unaligned_ratio', 'unaligned_ratio')],
                    1, min)
            print(head(d))

            a = seq(0, 3, 0.1)
            d.line = data.frame(xx=a, yy=rep_len(c(2.6, 2.8), length(a)))

            d.text = subset(d, filesystem == 'f2fs' & 
                            benchname %in% c('Sqlite', 'Varmail'))

            p = ggplot(d, aes(x=filesystem, y=limited_unaligned_ratio)) +
                geom_bar(stat='identity', position='dodge', width=0.8) +
                facet_grid(segment_bytes~benchname) +
                geom_hline(yintercept=1, size=0.3, color='black', linetype='dashed') +
                geom_path(data=d.line, aes(x=xx,y=yy), color='white', size=0.3,
                          linejoin='mitre')+
                geom_path(data=d.line, aes(x=xx,y=yy+0.05), color='white', size=0.3,
                          linejoin='mitre')+
                geom_path(data=d.line, aes(x=xx,y=yy+0.1), color='white', size=0.3,
                          linejoin='mitre')+
                geom_path(data=d.line, aes(x=xx,y=yy+0.15), color='white', size=0.3,
                          linejoin='mitre')+
                geom_path(data=d.line, aes(x=xx,y=yy+0.2), color='white', size=0.3,
                          linejoin='mitre')+
                geom_text(data=d.text,
                          aes(label=round(unaligned_ratio)), 
                              position=position_dodge(1), 
                              color='black', hjust=0.5, vjust=-0.1, size=3) +
                theme_zplot() +
                coord_cartesian(ylim=c(0, 3.5)) +
                scale_y_continuous(breaks=c(0, 1, 2)) +
                scale_fill_grey_dark() +
                ylab('Unaligned Ratio') +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                theme(panel.margin = unit(0.8, "lines"))

            print(p)
            save_plot(p, 'unaligned-size', w=3.2, h=2.1, save=T)
        }
    )
)



c_______Unaligned_Ratio_NEW____________ <- function(){}

SubExpAlignmentRatio <- setRefClass("SubExpAlignmentRatio",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            print(subexppath)
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
                benchname=confjson$get_testname_appname(),
                testname=confjson$get_testname(),
                rw=confjson$get_testname_rw(),
                pattern=confjson$get_testname_pattern(),
                appname=confjson$get_testname_appname(),
                rule=confjson$get_testname_rulename(),
                ext4hasjournal=confjson$get_ext4hasjournal(),
                flash_w_bytes=recjson$get_flash_ops_write() * pagesize,
                flash_r_bytes=recjson$get_flash_ops_read() * pagesize,
                flash_e_bytes=recjson$get_flash_ops_erase() * blocksize
                ))
        },
        get_alignment_filesystem = function(confjson)
        {
            fs = confjson$get_exp_parameters_filesystem()
            return(fs)
        }
    )
)

SubExpDirSize <- setRefClass("SubExpDirSize",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            confjson = create_configjson(subexppath=subexppath)
            statsjson = StatsJson(
                  filepath=paste(subexppath, 'stats.json', sep='/'))

            pagesize=as.numeric(confjson$get_page_size())
            blocksize=as.numeric(confjson$get_block_size())
            
            return(list(
                filesystem=get_alignment_filesystem(confjson),
                testname=confjson$get_testname(),
                disk_used_bytes=statsjson$get_disk_used_bytes()
                ))
        },
        get_alignment_filesystem = function(confjson)
        {
            fs = confjson$get_exp_parameters_filesystem()
            return(fs)
        }
    )
)



AligmentPlotBaseNew <- setRefClass("AligmentPlotBaseNew",
    fields = c('expname', 'df'),
    methods = list(
        plot = function(exp_rel_path, exp_rel_path_stats)
        {
            my_cache_path = cache_path(exp_rel_path, exp_rel_path_stats)
            print('-----')
            print(my_cache_path)
            if (cache_exists(exp_rel_path, exp_rel_path_stats))  {
                print('Using cache')
                print(my_cache_path)
                load(my_cache_path)
            } else {
                print('No cache')
                d = get_data(exp_rel_path, exp_rel_path_stats)
                print(my_cache_path)
                save(d, file=my_cache_path)
            }

            do_quick_plot(d)
        },

        do_quick_plot = function(d)
        {
            d = subset(d, rw=='w')

            d = cols_to_num(d, c('disk_used_bytes'))

            d = rename_pattern_levels(d)

            d = transform(d, 
                Unaligned=fullmerge_ops.physical_write + partialmerge_ops.physical_write)
            d = transform(d, Unaligned = Unaligned*pagesize)
            d = transform(d, unaligned_ratio = Unaligned / disk_used_bytes)

            d$blocksize = to_human_factor(as.numeric(as.character(d$blocksize)))

            d.line = get_white_lines()
            d.text.1 = subset(d, filesystem == 'f2fs' &   appname %in% c('sqliteRB', 'varmail'))
            d.text.2 = subset(d, filesystem == 'f2fs' &   appname %in% c('sqliteWAL') 
                              &  blocksize == '1 MB' & pattern != 'seq/S')

            block_colors = get_two_grays()
            names(block_colors) = c('128 KB', '1 MB')

            d = shorten_appnames(d)
            d.text.1 = shorten_appnames(d.text.1)
            d.text.2 = shorten_appnames(d.text.2)

            p = ggplot(d, aes(x=filesystem, y=unaligned_ratio)) +
                geom_bar(aes(fill=blocksize), stat='identity', position='dodge', width=0.8) +
                geom_hline(yintercept=1, size=0.2, color='black', linetype='dashed') +
                geom_path(data=d.line, aes(x=xx,y=yy,group=lineid), color='white', size=0.3,
                          linejoin='mitre')+
                geom_text(data=d.text.1,
                          aes(label=round(unaligned_ratio), y=2.9, color=blocksize),
                          position=position_dodge(1.3), 
                          hjust=0.5, vjust=-0.1, size=1.7,
                          show_guide=FALSE,
                              ) +
                geom_text(data=d.text.2,
                          aes(label=round(unaligned_ratio), y=2.9, color=blocksize),
                          x=2.25,
                          # hjust=-0.2, 
                          vjust=-0.1, size=1.7,
                          show_guide=FALSE
                              ) +
                coord_cartesian(ylim=c(0, 3.5)) +
                facet_grid(pattern~appname) +
                scale_y_continuous(breaks=c(0, 1, 2)) +
                theme_zplot() +
                scale_color_manual(values=block_colors) +
                scale_fill_manual(values=block_colors) +
                ylab('Unaligned Ratio') +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                theme(legend.key.size = unit(0.2, 'cm')) +
                theme(axis.title.x=element_blank()) +
                theme(legend.position=c(0.17, 0.9),
                      legend.direction="horizontal",
                      legend.background=element_blank()
                      )
                # theme(panel.margin = unit(0.1, "lines"))

            print(p)
            save_plot(p, 'unaligned-ratio', save=T, w=3.2, h=1.7)

        },
        get_white_lines = function()
        {
            d.line = NULL
            for ( i in seq(40)) {
                a = seq(0, 3, 0.1)
                d = data.frame(lineid=i, xx=a, yy=rep_len(c(2.6, 2.8) + i*0.05, length(a)))
                d.line = rbind(d.line, d)
            }
            return(d.line)
        },
        get_data = function(exp_rel_path, exp_rel_path_stats)
        {
            d = aggregate_subexp_list_to_dataframe(exp_rel_path, 
                                               SubExpAlignmentRatio)
            d.stats = aggregate_subexp_list_to_dataframe(exp_rel_path_stats, 
                                               SubExpDirSize)
            d = clean_data(d)

            d = merge(d, d.stats, by=c('testname', 'filesystem'))

            return(d)
        },
        filter_data_for_plot = function(d)
        {
            return(d)
        },
        clean_data = function(d)
        {
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
            return(d)
        },
        cache_path = function(exp_rel_path, exp_rel_path_stats)
        {
            exp_rel_path = gsub('/', '', exp_rel_path)
            exp_rel_path_stats = gsub('/', '', exp_rel_path_stats)
            long_path = paste(exp_rel_path, exp_rel_path_stats, collapse='-')
            cache_path = paste(digest(long_path), 'unalign-ratio', 'rdata', sep='.')
            return(cache_path)
        },
        cache_exists = function(exp_rel_path, exp_rel_path_stats)
        {
            path = cache_path(exp_rel_path, exp_rel_path_stats)
            return( file.exists(path) )
        }
    )
)





c_______Ext4_extent_stats__________ <- function(){}

ExtentStats <- setRefClass("ExtentStats",
    fields = list(),
    methods = list(
        run = function(exp_rel_path)
        {
            subexpiter = SubExpIter(exp_rel_path=exp_rel_path)
            result = subexpiter$iter_each_subexp(SubExpGcLogParsed)
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



c_______benchmark_classes__________ <- function(){}

paper_figures <- function()
{
    # unaligned data
    # alignmentbench = AligmentPlotBase()
    # alignmentbench$plot_unaligned_size(c(
                         # 'leveldb-6m-mixed',
                         # 'sqlite-1gplus-traffic',
                         # 'sqlite-1m-to-paper',
                         # 'varmail-20s-2fs',
                         # 'varmail-1mb-for-paper'
                         # ))

    # Traffic
    # alignmentbench = AligmentPlotBase()
    # alignmentbench$plot_traffic(c(
                         # 'leveldb-6m-mixed',
                         # 'sqlite-1gplus-traffic',
                         # 'sqlite-1m-to-paper',
                         # 'varmail-20s-2fs',
                         # 'varmail-1mb-for-paper'
                         # ))

    # NEW-----------------
    # exp with 4 fs
    # alignmentbench = AligmentPlotBase()
    unaligned_size = UnalignedSizePlot()
    unaligned_size$plot_unaligned_size(
                       c(
                        'leveldb-6m-4-fs-for-alignment',
                        'varmail-alignment-4fs',
                        'varmail-btrfs-128kb-makeup',
                        'sqlite-alignment-4fs'
                         )
                       )

    # Traffic
#     traffic = TrafficSizePlot()
    # traffic$plot_traffic(c(
                        # 'leveldb-6m-4-fs-for-alignment',
                        # 'varmail-alignment-4fs',
                        # 'varmail-btrfs-128kb-makeup',
                        # 'sqlite-alignment-4fs'
                         # ))

    # # Ratio
    # unaligned_ratio = UnalignedRatioPlot_PAPER()
    # unaligned_ratio$plot(
                       # c(
                        # 'leveldb-6m-4-fs-for-alignment',
                        # 'varmail-alignment-4fs',
                        # 'varmail-btrfs-128kb-makeup',
                        # 'sqlite-alignment-4fs'
                         # )
                       # )

}

plot_set <- function(dirpaths)
{
    unaligned_size = UnalignedSizePlot()
    unaligned_size$plot_unaligned_size(dirpaths)

    # Traffic
    traffic = TrafficSizePlot()
    traffic$plot_traffic(dirpaths)


    unaligned_ratio = UnalignedRatioPlot_PAPER()
    unaligned_ratio$plot(dirpaths)
}

main <- function()
{

    ratios = AligmentPlotBaseNew()
    ratios$plot(
                    c(
                        "alignment-rocksdb-tracesim-segnoseg-100gb",
                        "alignment-leveldb-tracesim-segnoseg-100gb",
                        "alignment-sqlitewal-tracesim-segnoseg-100gb",
                        "sqliterb-alignment-makeup3",
                        "alignment-varmail-tracesim-segnoseg-100gb"
                      ),
                    c( 
                         'rocksdb-reqscale',
                         'leveldb-reqscale-001',
                         'sqliterb-reqscale-240000-insertions-4',
                         'sqlitewal-reqscale-240000-inserts-3',
                         'varmail-reqscale-002'
                     )
                    )

}

main()












