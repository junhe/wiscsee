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
    THISFILE     = 'doraemon/analyze-scaling.r'
    setwd(WORKDIRECTORY)
    source(THISFILE)
}

theme_boxplot <- function()
{
    t = theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
        theme(axis.title.x=element_blank()) +
        theme(legend.key.size = unit(0.3, 'cm')) +
        theme(legend.position=c(0.5, 0.9), 
              legend.direction="horizontal",
              legend.title=element_blank()) +
        theme(plot.margin = unit(c(0, 0, -0.2, 0), 'cm'))

    return(t)
}



c_____FSYNC_SIZE_TABLE_DIRTYTABLE______________ <- function(){}

SubExpFsyncDirtyTable <- setRefClass("SubExpFsyncDirtyTable",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)

            fsynctable <- DirtyTable(
                  filepath=paste(subexppath, '3.strace.out.dirty_table', sep='/'))
            d = fsynctable$load()

            d = transform(d, time = time - min(time))
            d = transform(d, dirty_size = dirty_size / MB)
            
            print('total')
            print(sum(d$dirty_size))

            plot_timeline(d)

            return()
        }, 
        plot_timeline = function(d)
        {
     # callname       time   pid dirty_size                                       filepath
# 1   fdatasync 1474601241 11483         16    /mnt/fsonloop/appmix/0-LevelDB/000001.dbtmp
# 2       fsync 1474601241 11483          0                 /mnt/fsonloop/appmix/0-LevelDB
# 3   fdatasync 1474601241 11483         50 /mnt/fsonloop/appmix/0-LevelDB/MANIFEST-000002
            p = ggplot(d, aes(x=time, y=dirty_size)) +
                geom_point()
            print_plot(p)
        }
    )
)

FsyncTimeline <- setRefClass("FsyncTimeline",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpFsyncDirtyTable)

        }
    )
)


c_____BLK_REQ_SIZE_STATS______________ <- function(){}

SubExpBlkEvents <- setRefClass("SubExpBlkEvents",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            blk_events = BlkParseEvents(
                  filepath=paste(subexppath, 'blkparse-events-for-ftlsim.txt', sep='/'))

            d = blk_events$load()
            d = transform(d, 
                          expname=confjson$get_expname(),
                          n_insts=confjson$get_appmix_n_insts(),
                          filesystem=confjson$get_filesystem(),
                          appname=confjson$get_testname_appname(),
                          fs_discard=confjson$get_appmix_fs_discard(),
                          testname=confjson$get_testname(),
                          rw=confjson$get_testname_rw(),
                          pattern=confjson$get_testname_pattern()
                          )


            d = transform(d, myexpname = confjson$get_appmix_myexpname())
            d = transform(d, size = as.numeric(size))

            return(d)
        }
    )
)

BlkReqSizeCDF_EXPLORE <- setRefClass("BlkReqSizeCDF_EXPLORE",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpBlkEvents)
            d = subset(d, action == 'D')

            do_cdf(d)
            # do_cdf_for_paper(d)
        },
        do_cdf = function(d)
        {
            d = subset(d, operation != 'discard')
            p = ggplot(d, aes(x=size/KB, color=operation)) + 
                stat_ecdf() +
                facet_grid(myexpname~.)
            print_plot(p)
        }
    )
)



BlkReqSizeCDF_4_PAPER <- setRefClass("BlkReqSizeCDF_4_PAPER",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpBlkEvents)
            d = subset(d, action == 'D')

            do_cdf_for_paper(d)
        },
        do_cdf_for_paper = function(d)
        {
            d = subset(d, operation != 'discard')
            d = transform(d, size = size / KB)
            d = subset(d, size > 0)
            print('min size')
            print(min(d$size))
            d = order_fs_levels(d)
            p = ggplot(d, aes(x=size, color=filesystem)) + 
                stat_ecdf(size=0.3) +
                # coord_cartesian(xlim=c(0, 2000)) +
                xlab('Request Size (KB)') +
                ylab('Cumulative Density') +
                scale_color_manual(values=get_fs_color_map()) +
                facet_grid(~appname) +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            print_plot(p)
            save_plot(p, 'sqlite-req-size-ecdf', save=T, w=3.2, h=2.5)
        }
    )
)


Req_Size_Box <- setRefClass("Req_Size_Box",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            if (cache_exists(exp_rel_path))  {
                print('Using cache')
                print(cache_path(exp_rel_path))
                load(cache_path(exp_rel_path))
            } else {
                print('No cache----here')
                d = aggregate_subexp_dataframes(exp_rel_path, SubExpBlkEvents)
                d = prepare_box_data(d)
                print(cache_path(exp_rel_path))
                save(d, file=cache_path(exp_rel_path))
            }

            do_quick_plot(d)
        },
        prepare_box_data = function(d)
        {
            ddply_boxplot_stats <- function(d) {
                stats = boxplot.stats(d$size)$stats
                stats = t(stats)
                stats = data.frame(stats)
                names(stats) = c('bx.ymin', 'bx.lower', 
                                 'bx.middle', 'bx.upper', 'bx.ymax')

                dd = head(d, 1)
                dd = cbind(dd, stats)
                return(dd)
            }

            print('preparing box data')

            d = subset(d, action == 'D')
            d = subset(d, operation != 'discard')
            d = transform(d, size = size / KB)
            d = subset(d, size > 0)
            d = order_fs_levels(d)
            d = order_pattern_levels(d)
            d = order_rw_levels(d)

            d = ddply(d, .(pattern, filesystem, rw, appname), ddply_boxplot_stats)

            return(d)
        },
        do_quick_plot = function(d)
        {
            # d = rename_sqlite(d)
            d = shorten_appnames(d)
            d$rw = revalue(d$rw, c("r"="read", "w"="write"))
            p = ggplot(d, aes(x=pattern, color=filesystem)) + 
                geom_boxplot(
                    aes(ymin = bx.ymin, lower = bx.lower, middle = bx.middle, 
                        upper = bx.upper, ymax = bx.ymax),
                    stat='identity',
                    outlier.size=NA, width=0.6, size=0.25, position=position_dodge(0.7)) +
                # coord_cartesian(xlim=c(0, 2000)) +
                ylab('Request Size (KB)') +
                scale_color_manual(values=get_fs_color_map()) +
                expand_limits(y=0) +
                facet_grid(rw~appname, scales='free_x', space='free_x') +
                theme_zplot() +
                theme_boxplot()
            print_plot(p)
            save_plot(p, 'req-size-box', save=T, w=3.4, h=1.5)
        },
        cache_path = function(exp_rel_path)
        {
            exp_rel_path = gsub('/', '', exp_rel_path)
            exp_rel_path = paste(exp_rel_path, collapse='-')
            cache_path = paste(exp_rel_path, 'req_size_box', 'rdata', sep='.')
            return(cache_path)
        },
        cache_exists = function(exp_rel_path)
        {
            path = cache_path(exp_rel_path)
            return( file.exists(path) )
        }
    )
)


Avg_Req_Size <- setRefClass("Avg_Req_Size",
    fields = list(expname='character'),
    contains = c('PlotWithCache'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = get_data(exp_rel_path, classname='avg-req-size')

            do_quick_plot(d)
        },
        get_raw_data = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpBlkEvents)

            d = subset(d, action == 'D')
            d = transform(d, size = size / KB)
            d = subset(d, size > 0)
            d = order_fs_levels(d)
            d = order_pattern_levels(d)
            d = order_rw_levels(d)

            d = aggregate(size~pattern+filesystem+rw+appname+operation, data=d, mean)

            return(d)
        },
        do_quick_plot = function(d)
        {
            d = shorten_appnames(d)
            d = subset(d, operation != 'discard')
            p = ggplot(d, aes(x=pattern, y=size, color=operation, fill=filesystem)) + 
                geom_bar(stat='identity', position='dodge') +
                geom_text(aes(label=round(size)), position=position_dodge(1), size=3,
                          angle=90, hjust=-0.1
                          ) +
                # coord_cartesian(xlim=c(0, 2000)) +
                ylab('Avg Request Size (KB)') +
                scale_fill_manual(values=get_fs_color_map()) +
                expand_limits(y=0) +
                facet_grid(rw~appname, scales='free_x', space='free_x') +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                theme(axis.title.x=element_blank())
            print_plot(p)
            save_plot(p, 'avg-req-size', save=T, w=10, h=6)
        }
    )
)



Req_Size_Box_comppattern <- setRefClass("Req_Size_Box_comppattern",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            if (cache_exists(exp_rel_path))  {
                print('Using cache')
                print(cache_path(exp_rel_path))
                load(cache_path(exp_rel_path))
            } else {
                print('No cache')
                d = aggregate_subexp_dataframes(exp_rel_path, SubExpBlkEvents)
                d = prepare_box_data(d)
                print(cache_path(exp_rel_path))
                save(d, file=cache_path(exp_rel_path))
            }

            do_quick_plot(d)
        },
        prepare_box_data = function(d)
        {
            ddply_boxplot_stats <- function(d) {
                stats = boxplot.stats(d$size)$stats
                stats = t(stats)
                stats = data.frame(stats)
                names(stats) = c('bx.ymin', 'bx.lower', 
                                 'bx.middle', 'bx.upper', 'bx.ymax')

                dd = head(d, 1)
                dd = cbind(dd, stats)
                return(dd)
            }

            d = subset(d, action == 'D')
            d = subset(d, operation != 'discard')
            d = transform(d, size = size / KB)
            d = subset(d, size > 0)
            d = order_fs_levels(d)
            d = order_pattern_levels(d)
            d = order_rw_levels(d)

            d = ddply(d, .(pattern, filesystem, rw, appname), ddply_boxplot_stats)

            return(d)
        },
        do_quick_plot = function(d)
        {
            d = shorten_appnames(d)
            d = rename_pattern_levels(d)

            p = ggplot(d, aes(x=filesystem, color=pattern)) + 
                geom_boxplot(
                    aes(ymin = bx.ymin, lower = bx.lower, middle = bx.middle, 
                        upper = bx.upper, ymax = bx.ymax),
                    stat='identity',
                    outlier.size=NA, width=0.9, size=0.3, position=position_dodge(0.9)) +
                # coord_cartesian(xlim=c(0, 2000)) +
                ylab('Request Size (KB)') +
                # scale_color_manual(values=get_fs_color_map()) +
                scale_color_grey_dark() +
                expand_limits(y=0) +
                facet_grid(rw~appname, scales='free_x', space='free_x') +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                theme(axis.title.x=element_blank())
            print_plot(p)
            save_plot(p, 'req-size-box', save=T, w=3.2, h=2.5)
        },
        cache_path = function(exp_rel_path)
        {
            exp_rel_path = gsub('/', '', exp_rel_path)
            exp_rel_path = paste(exp_rel_path, collapse='-')
            cache_path = paste(exp_rel_path, 'req_size_box', 'rdata', sep='.')
            return(cache_path)
        },
        cache_exists = function(exp_rel_path)
        {
            path = cache_path(exp_rel_path)
            return( file.exists(path) )
        }
    )
)






Discard_Ratio <- setRefClass("Discard_Ratio",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            if (cache_exists(exp_rel_path))  {
                print('Using cache for discard ratio')
                print(cache_path(exp_rel_path))
                load(cache_path(exp_rel_path))
            } else {
                print('No cache')
                d = aggregate_subexp_dataframes(exp_rel_path, SubExpBlkEvents)
                d = prepare_data(d)
                save(d, file=cache_path(exp_rel_path))
            }

            discard_count(d)
        },
        prepare_data = function(d)
        {
            d = subset(d, action == 'D') # we need both read and write for req scale
            d$count = 1
            d = aggregate(count~filesystem+appname+operation+rw+pattern, data=d, sum)
            d = dcast(d, filesystem+appname+rw+pattern~operation, value.var='count')

            d = transform(d, discard_ratio = discard / (read + write + discard))

            d = order_fs_levels(d)
            d = order_pattern_levels(d)
            d = order_rw_levels(d)

            return(d)
        },
        discard_count = function(d)
        {
            d = subset(d, rw == 'w')
            d = shorten_appnames(d)
            d$rw = revalue(d$rw, c("r"="read", "w"="write"))
            p = ggplot(d, aes(x=pattern, y=discard_ratio, fill=filesystem)) + 
                geom_bar(stat='identity', position='dodge', width=0.7) +
                ylab('Discard Ratio') +
                scale_fill_manual(values=get_fs_color_map()) +
                facet_grid(rw~appname, scales='free_x', space='free_x') +
                theme_zplot() +
                theme_boxplot() +
                theme(strip.text.x = element_text(size=7)) +
                theme(legend.position=c(0.5, 0.7))



            print(p)
            save_plot(p, 'discard-count', save=T, w=3.2, h=1)
        },
        cache_path = function(exp_rel_path)
        {
            exp_rel_path = gsub('/', '', exp_rel_path)
            exp_rel_path = paste(exp_rel_path, collapse='-')
            cache_path = paste(exp_rel_path, 'discard_ratio', 'rdata', sep='.')

            return(cache_path)
        },
        cache_exists = function(exp_rel_path)
        {
            path = cache_path(exp_rel_path)
            return( file.exists(path) )
        }
 
    )
)

Discard_Ratio_comppattern <- setRefClass("Discard_Ratio_comppattern",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            if (cache_exists(exp_rel_path))  {
                print('Using cache')
                load(cache_path(exp_rel_path))
            } else {
                print('No cache')
                d = aggregate_subexp_dataframes(exp_rel_path, SubExpBlkEvents)
                d = prepare_data(d)
                save(d, file=cache_path(exp_rel_path))
            }

            discard_count(d)
        },
        prepare_data = function(d)
        {
            d = subset(d, action == 'D') # we need both read and write for req scale
            d$count = 1
            d = aggregate(count~filesystem+appname+operation+rw+pattern, data=d, sum)
            d = dcast(d, filesystem+appname+rw+pattern~operation, value.var='count')

            d = transform(d, discard_ratio = discard / (read + write + discard))

            d = order_fs_levels(d)
            d = order_pattern_levels(d)
            d = order_rw_levels(d)

            return(d)
        },
        discard_count = function(d)
        {
            d = subset(d, rw == 'w')
            d = rename_pattern_levels(d)
            d = shorten_appnames(d)
            p = ggplot(d, aes(x=filesystem, y=discard_ratio, fill=pattern)) + 
                geom_bar(stat='identity', position='dodge', width=0.7) +
                ylab('Discard Ratio') +
                scale_fill_grey_dark() +
                facet_grid(rw~appname, scales='free_x', space='free_x') +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                theme(axis.title.x=element_blank())

            print(p)
            save_plot(p, 'discard-count', save=T, w=3.2, h=1.8)
        },
        cache_path = function(exp_rel_path)
        {
            exp_rel_path = gsub('/', '', exp_rel_path)
            exp_rel_path = paste(exp_rel_path, collapse='-')
            cache_path = paste(exp_rel_path, 'discard_ratio', 'rdata', sep='.')

            return(cache_path)
        },
        cache_exists = function(exp_rel_path)
        {
            path = cache_path(exp_rel_path)
            return( file.exists(path) )
        }
 
    )
)



NumProcs <- setRefClass("NumProcs",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpBlkEvents)
            d = subset(d, action == 'D')

            d = remove_dup(d, c('expname', 'pid'))
            d = transform(d, pidcount = 1)

            d = aggregate(pidcount~expname, data=d, sum)
            print(d)
        }
    )
)

BlkReqSizeCDF_PAPER <- setRefClass("BlkReqSizeCDF_PAPER",
    fields = list(expname='character'),
    methods = list(
        plot = function()
        {
            d.leveldb = get_leveldb()
            d.sqlite = get_sqlite()
            d.varmail = get_varmail()

            d = rbind(d.leveldb, d.sqlite, d.varmail)

            # d = d.leveldb

            do_cdf_for_paper(d)
            # do_discard_count(d)
        },
        get_leveldb = function()
        {
            # ext4
            d.ext4 = aggregate_subexp_dataframes('leveldb-10m-4inst', SubExpBlkEvents)
            d.ext4$fs_discard = TRUE

            # f2fs
            d.f2fs = aggregate_subexp_dataframes('leveldb-10m-f2fs-4inst', SubExpBlkEvents)
            d.f2fs$fs_discard = TRUE

            # xfs
            d.xfs = aggregate_subexp_dataframes('leveldb-xfs-4-inst-gogo', SubExpBlkEvents)
            d.xfs$fs_discard = TRUE

            d = rbind(d.ext4, d.f2fs, d.xfs)

            return(d)
        },
        get_sqlite = function()
        {
            # ext4
            d.ext4 = aggregate_subexp_dataframes('sqlite-ext4-4inst-go', SubExpBlkEvents)
            d.ext4$fs_discard = TRUE

            # f2fs
            d.f2fs = aggregate_subexp_dataframes(
                 'sqlite-f2fs-1-4-insts-discard-T-F/subexp--3733746757608966617-f2fs-09-23-22-50-41-5912988510877335738', 
                 SubExpBlkEvents)

            # xfs
            d.xfs = aggregate_subexp_dataframes(
                'sqlite-xfs-discard-nodiscard-1-4-inst/subexp--1277215920900524584-xfs-09-24-09-27-37--4114517400537133574', SubExpBlkEvents)

            d = rbind(d.ext4, d.f2fs, d.xfs)

            return(d)

        },
        get_varmail = function()
        {
            d = aggregate_subexp_dataframes("varmail-3fs-discard-T-F-1-4-fastfastext4", 
                    SubExpBlkEvents)

            d = subset(d, fs_discard == TRUE & n_insts == 4)
            return(d)
        },
 
        do_cdf_for_paper = function(d)
        {
            # d = subset(d, operation == 'write' & action == 'D')
            d = subset(d, action == 'D') # we need both read and write for req scale
            d = transform(d, size = size / KB)

            # save(d, file='reqsize-cdf-df.rdata') 

            p = ggplot(d, aes(x=size, color=filesystem)) + 
                stat_ecdf(size=0.3) +
                # coord_cartesian(xlim=c(0, 2000)) +
                xlab('Write Request Size (KB)') +
                ylab('Cumulative Density') +
                scale_color_manual(values=get_fs_color_map()) +
                facet_grid(~appname) +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            print_plot(p)
            save_plot(p, 'req-size-ecdf', save=T, w=3.2, h=2.5)
        },

        do_discard_count = function(d) 
        {
            d = subset(d, action == 'D') # we need both read and write for req scale
            d = subset(d, operation == 'discard')
            d$count = 1
            d = aggregate(count~filesystem+appname, data=d, sum)
            
            print(d)

            p = ggplot(d, aes(x=filesystem, y=count, fill=filesystem)) + 
                geom_bar(stat='identity', width=0.7) +
                xlab('File System') +
                ylab('Num of Discards') +
                # scale_x_continuous(limits=c(-1, xmax * 1.1)) +
                scale_fill_manual(values=get_fs_color_map()) +
                facet_grid(~appname) +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))

            print(p)
            save_plot(p, 'discard-count', save=T, w=3.2, h=2.5)
        },
 
        ending_______________ = function() {}
    )
)



c_____OP_TYPE______________ <- function(){}

SubExpReqDataType <- setRefClass("SubExpReqDataType",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            blk_events = BlkParseEvents(
                  filepath=paste(subexppath, 'blkparse-events-for-ftlsim.txt', sep='/'))
            lpnsem <- LpnSem(
                  filepath=paste(subexppath, 'lpn_sem.out', sep='/'))
            depth_timeline = NcqDepthTimelineTable(
                  filepath=paste(subexppath, 'ncq_depth_timeline.txt', sep='/'))

            d.events = blk_events$load()
            d.sem = lpnsem$load()
            d.sem = replace_sem(d.sem)
            d.depth = depth_timeline$load()

            print(head(d.events))
            print(head(d.sem))
            print(head(d.depth))

            d.events = cols_to_num(d.events, c('offset'))

            d.events = transform(d.events, lpn = offset / (2*KB))

            d = merge(d.events, d.sem)

            d = transform(d, expname = confjson$get_expname())
            d = transform(d, myexpname = confjson$get_appmix_myexpname())
            d = transform(d, size = as.numeric(size))

            print(head(d))

            return(d)
        },
        replace_sem = function(d)
        {
            if (confjson$get_filesystem() == 'ext4') {
                lvs = levels(d$sem)

                sel1 = grepl('appmix', lvs)
                sel2 = grepl('None', lvs)
                sel = sel1 | sel2
                lvs[sel] = 'Data-region'

                lvs = gsub('journal', 'Journal', lvs, fixed=T)
                lvs = gsub('inode-table', 'Inode-table', lvs, fixed=T)
                lvs = gsub('groupdesc', 'GroupDesc', lvs, fixed=T)
                lvs = gsub('superblock', 'Superblock', lvs, fixed=T)

                levels(d$sem) = lvs

            } else if (confjson$get_filesystem() == 'xfs') {
                lvs = levels(d$sem)

                non_metadata = !lvs %in% c('Journal',
                                'Superblock',
                                'FreeBlockInfo',
                                'InodeInfo',
                                'FreeListInfo',
                                'FreeSpTree1Root',
                                'FreeSpTree2Root',
                                'InodeTreeRoot',
                                'FreeList')
                lvs[non_metadata] = 'Data-region'

                ag_header = lvs %in% c(
                                'Superblock',
                                'FreeBlockInfo',
                                'InodeInfo',
                                'FreeListInfo',
                                'FreeSpTree1Root',
                                'FreeSpTree2Root',
                                'InodeTreeRoot',
                                'FreeList'
                                )
 
                lvs[ag_header] = 'AG-header'

                # lvs = gsub('journal', 'Journal', lvs, fixed=T)

                levels(d$sem) = lvs

            } else if (confjson$get_filesystem() == 'f2fs') {
                lvs = levels(d$sem)

                non_metadata = !lvs %in% c('Journal',
                    'Superblock',
                    'Checkpoint',
                    'SegInfoTab',
                    'NodeAddrTab',
                    'SegSummArea')

                lvs[non_metadata] = 'Data-region'

                levels(d$sem) = lvs
            }
            return(d)
        },

        ending_______________ = function() {}

    )
)

ReqDataTypeStats  <- setRefClass("ReqDataTypeStats",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpReqDataType)
            
            do_cdf(d)
        },
        do_cdf = function(d)
        {
            d = subset(d, operation == 'write')
            p = ggplot(d, aes(x=size/KB, color=sem)) + 
                stat_ecdf() +
                facet_grid(myexpname~operation)
            print_plot(p)
        },
        ending_______________ = function() {}
    )
)

SizeBySem  <- setRefClass("SizeBySem",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpReqDataType)

            d = subset(d, action == 'D')

            d = aggregate(size~sem+myexpname, data=d, sum)
            print(d)
           p = ggplot(d, aes(x=sem, y=size/MB)) +
                geom_bar(stat='identity') +
                facet_grid(myexpname~.)
            print_plot(p)
        },
 
        ending_______________ = function() {}
    )
)


c_____NCQ_Depth_Boxplot______________ <- function(){}

SubExpDepthOnly <- setRefClass("SubExpDepthOnly",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)

            depth_timeline = NcqDepthTimelineTable(
                  filepath=paste(subexppath, 'ncq_depth_timeline.txt', sep='/'))

            d = depth_timeline$load()
            d = cols_to_num(d, c('offset'))

            d = transform(d, expname = confjson$get_expname())
            d = transform(d, myexpname = confjson$get_appmix_myexpname())

            print(confjson$get_testname())
            d = transform(d, 
                          n_insts=confjson$get_appmix_n_insts(),
                          filesystem=confjson$get_filesystem(),
                          appname=confjson$get_testname_appname(),
                          fs_discard=confjson$get_appmix_fs_discard(),
                          testname=confjson$get_testname(),
                          rw=confjson$get_testname_rw(),
                          pattern=confjson$get_testname_pattern()
                          )

            # adjust 
            min_pre_depth = min(d$pre_depth)
            d$pre_depth = d$pre_depth - (min_pre_depth)

            return(d)
        },
        ending_______________ = function() {}

    )
)

NCQ_Depth_Box  <- setRefClass("NCQ_Depth_Box",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            if (cache_exists(exp_rel_path)) {
                print('Using cache for Depth')
                print(cache_path(exp_rel_path))
                load(cache_path(exp_rel_path))
            } else {
                print('NO cache')
                d = aggregate_subexp_dataframes(exp_rel_path, SubExpDepthOnly)
                d = prepare_box_data(d)
                save(d, file=cache_path(exp_rel_path))
            }
            print(summary(d))
            
            do_quick_plot(d)
        },
        prepare_box_data = function(d)
        {
            ddply_boxplot_stats <- function(d) {
                stats = boxplot.stats(d$pre_depth)$stats
                stats = t(stats)
                stats = data.frame(stats)
                names(stats) = c('bx.ymin', 'bx.lower', 
                                 'bx.middle', 'bx.upper', 'bx.ymax')

                dd = head(d, 1)
                dd = cbind(dd, stats)
                return(dd)
            }

            d = subset(d, operation != 'discard' & action == 'D')
            d = order_fs_levels(d)
            d = order_pattern_levels(d)
            d = order_rw_levels(d)

            d = ddply(d, .(pattern, filesystem, rw, appname), ddply_boxplot_stats)

            return(d)
        },
        do_quick_plot = function(d)
        {
           # d = rename_sqlite(d)
           d = shorten_appnames(d)
           d$rw = revalue(d$rw, c("r"="read", "w"="write"))
           p = ggplot(d, aes(x=pattern, color=filesystem)) + 
                geom_boxplot(
                    aes(ymin = bx.ymin, lower = bx.lower, middle = bx.middle, 
                        upper = bx.upper, ymax = bx.ymax),
                    stat='identity',
                    outlier.size=NA, width=0.6, size=0.25, position=position_dodge(0.7)) +
                ylab('NCQ Depth') +
                scale_color_manual(values=get_fs_color_map()) +
                expand_limits(y=0) +
                facet_grid(rw~appname, scales='free_x') +
                theme_zplot() +
                theme_boxplot() +
                theme(legend.position='none')
            print_plot(p)

            save_plot(p, 'ncq-depth-box', save=T, w=3.4, h=1.5)
        },
        do_plot = function(d)
        {
           p = ggplot(d, aes(x=pattern, y=pre_depth, color=filesystem)) + 
                geom_boxplot(outlier.size=NA, width=0.9, lwd=0.1, position=position_dodge(0.9)) +
                # coord_cartesian(xlim=c(0, 2000)) +
                ylab('NCQ Depth') +
                scale_color_manual(values=get_fs_color_map()) +
                expand_limits(y=0) +
                facet_grid(rw~appname, scales='free_x') +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            print_plot(p)
        },
        cache_path = function(exp_rel_path)
        {
            exp_rel_path = gsub('/', '', exp_rel_path)
            exp_rel_path = paste(exp_rel_path, collapse='-')
            cache_path = paste(exp_rel_path, 'ncq_depth_box', 'rdata', sep='.')

            return(cache_path)
        },
        cache_exists = function(exp_rel_path)
        {
            path = cache_path(exp_rel_path)
            return( file.exists(path) )
        },
        ending_______________ = function() {}
    )
)




NCQ_Depth_Box_comppattern  <- setRefClass("NCQ_Depth_Box_comppattern",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            if (cache_exists(exp_rel_path)) {
                print('Using cache')
                load(cache_path(exp_rel_path))
            } else {
                print('NO cache')
                d = aggregate_subexp_dataframes(exp_rel_path, SubExpDepthOnly)
                d = prepare_box_data(d)
                save(d, file=cache_path(exp_rel_path))
            }
            print(summary(d))
            
            do_quick_plot(d)
        },
        prepare_box_data = function(d)
        {
            ddply_boxplot_stats <- function(d) {
                stats = boxplot.stats(d$pre_depth)$stats
                stats = t(stats)
                stats = data.frame(stats)
                names(stats) = c('bx.ymin', 'bx.lower', 
                                 'bx.middle', 'bx.upper', 'bx.ymax')

                dd = head(d, 1)
                dd = cbind(dd, stats)
                return(dd)
            }

            d = subset(d, operation != 'discard' & action == 'D')
            d = order_fs_levels(d)
            d = order_pattern_levels(d)
            d = order_rw_levels(d)

            d = ddply(d, .(pattern, filesystem, rw, appname), ddply_boxplot_stats)

            return(d)
        },
        do_quick_plot = function(d)
        {
           d = rename_pattern_levels(d)
           d = shorten_appnames(d)
           p = ggplot(d, aes(x=filesystem, color=pattern)) + 
                geom_boxplot(
                    aes(ymin = bx.ymin, lower = bx.lower, middle = bx.middle, 
                        upper = bx.upper, ymax = bx.ymax),
                    stat='identity',
                    outlier.size=NA, width=0.9, size=0.3, position=position_dodge(0.9)) +
                ylab('NCQ Depth') +
                # scale_color_manual(values=get_fs_color_map()) +
                scale_color_grey_dark() +
                expand_limits(y=0) +
                facet_grid(rw~appname, scales='free_x') +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                theme(axis.title.x=element_blank())
            print_plot(p)

            save_plot(p, 'ncq-depth-box', save=T, w=3.2, h=2.5)
        },
        do_plot = function(d)
        {
           p = ggplot(d, aes(x=pattern, y=pre_depth, color=filesystem)) + 
                geom_boxplot(outlier.size=NA, width=0.9, lwd=0.1, position=position_dodge(0.9)) +
                # coord_cartesian(xlim=c(0, 2000)) +
                ylab('NCQ Depth') +
                scale_color_manual(values=get_fs_color_map()) +
                expand_limits(y=0) +
                facet_grid(rw~appname, scales='free_x') +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            print_plot(p)
        },
        cache_path = function(exp_rel_path)
        {
            exp_rel_path = gsub('/', '', exp_rel_path)
            exp_rel_path = paste(exp_rel_path, collapse='-')
            cache_path = paste(exp_rel_path, 'ncq_depth_box', 'rdata', sep='.')

            return(cache_path)
        },
        cache_exists = function(exp_rel_path)
        {
            path = cache_path(exp_rel_path)
            return( file.exists(path) )
        },
        ending_______________ = function() {}
    )
)


c_____NCQ_Depth_Median______________ <- function(){}

SubExpDepthQuick <- setRefClass("SubExpDepthQuick",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)

            depth_timeline = NcqDepthTimelineTable(
                  filepath=paste(subexppath, 'ncq_depth_timeline.txt', sep='/'))

            d = depth_timeline$load()
            d = cols_to_num(d, c('offset'))

            d = transform(d, expname = confjson$get_expname())
            d = transform(d, myexpname = confjson$get_appmix_myexpname())

            print(confjson$get_testname())
            d = transform(d, 
                          n_insts=confjson$get_appmix_n_insts(),
                          filesystem=confjson$get_filesystem(),
                          appname=confjson$get_testname_appname(),
                          fs_discard=confjson$get_appmix_fs_discard(),
                          testname=confjson$get_testname(),
                          rw=confjson$get_testname_rw(),
                          pattern=confjson$get_testname_pattern()
                          )

            # adjust 
            min_pre_depth = min(d$pre_depth)
            d$pre_depth = d$pre_depth - (min_pre_depth)

            d = calcuate_quantiles(d)

            return(d)
        },
        calcuate_quantiles = function(d)
        {
            stats = boxplot.stats(d$pre_depth)$stats
            stats = t(stats)
            stats = data.frame(stats)
            names(stats) = c('bx.ymin', 'bx.lower', 
                             'bx.middle', 'bx.upper', 'bx.ymax')

            dd = head(d, 1)
            dd = cbind(dd, stats)
            return(dd)
        },
        ending_______________ = function() {}

    )
)

NCQ_Depth_Median_Table  <- setRefClass("NCQ_Depth_Median_Table",
    fields = list(expname='character'),
    contains = c('PlotWithCache'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = get_data(exp_rel_path, classname='median-table', 
                         try_cache = TRUE)
            
            do_quick_plot(d)
        },
        get_raw_data = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpDepthQuick)
            return(d)
        },
        do_quick_plot = function(d)
        {
            d = d[, c('testname', 'filesystem', 'bx.middle')]
            d = plyr::rename(d, c('bx.middle'='depth.median'))
            write.csv(d, file = 'ncq-median.csv', row.names=F)
        },
        ending_______________ = function() {}
    )
)


c_____NCQ_Depth_CDF______________ <- function(){}

SubExpBarrier <- setRefClass("SubExpBarrier",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)

            lpnsem <- LpnSem(
                  filepath=paste(subexppath, 'lpn_sem.out', sep='/'))
            depth_timeline = NcqDepthTimelineTable(
                  filepath=paste(subexppath, 'ncq_depth_timeline.txt', sep='/'))

            d.sem = lpnsem$load()
            d.sem = replace_sem(d.sem)
            d.depth = depth_timeline$load()

            d.depth = cols_to_num(d.depth, c('offset'))

            d.depth = transform(d.depth, lpn = offset / (2*KB))

            d = merge(d.depth, d.sem)

            d = transform(d, expname = confjson$get_expname())
            d = transform(d, myexpname = confjson$get_appmix_myexpname())
            d = transform(d, size = as.numeric(size))

            d = transform(d, 
                          n_insts=confjson$get_appmix_n_insts(),
                          filesystem=confjson$get_filesystem(),
                          appname=confjson$get_testname_appname(),
                          fs_discard=confjson$get_appmix_fs_discard(),
                          testname=confjson$get_testname(),
                          rw=confjson$get_testname_rw(),
                          pattern=confjson$get_testname_pattern()
                          )

            # adjust 
            min_pre_depth = min(d$pre_depth)
            d$pre_depth = d$pre_depth - (min_pre_depth)

            return(d)
        },
        replace_sem = function(d)
        {
            if (confjson$get_filesystem() == 'ext4') {
                lvs = levels(d$sem)

                sel1 = grepl('appmix', lvs)
                sel2 = grepl('None', lvs)
                sel = sel1 | sel2
                lvs[sel] = 'Data-region'

                lvs = gsub('journal', 'Journal', lvs, fixed=T)
                lvs = gsub('inode-table', 'Inode-table', lvs, fixed=T)
                lvs = gsub('groupdesc', 'GroupDesc', lvs, fixed=T)
                lvs = gsub('superblock', 'Superblock', lvs, fixed=T)

                levels(d$sem) = lvs

            } else if (confjson$get_filesystem() == 'xfs') {
                lvs = levels(d$sem)

                non_metadata = !lvs %in% c('Journal',
                                'Superblock',
                                'FreeBlockInfo',
                                'InodeInfo',
                                'FreeListInfo',
                                'FreeSpTree1Root',
                                'FreeSpTree2Root',
                                'InodeTreeRoot',
                                'FreeList')
                lvs[non_metadata] = 'Data-region'

                ag_header = lvs %in% c(
                                'Superblock',
                                'FreeBlockInfo',
                                'InodeInfo',
                                'FreeListInfo',
                                'FreeSpTree1Root',
                                'FreeSpTree2Root',
                                'InodeTreeRoot',
                                'FreeList'
                                )
 
                lvs[ag_header] = 'AG-header'

                # lvs = gsub('journal', 'Journal', lvs, fixed=T)

                levels(d$sem) = lvs

            } else if (confjson$get_filesystem() == 'f2fs') {
                lvs = levels(d$sem)

                non_metadata = !lvs %in% c('Journal',
                    'Superblock',
                    'Checkpoint',
                    'SegInfoTab',
                    'NodeAddrTab',
                    'SegSummArea')

                lvs[non_metadata] = 'Data-region'

                levels(d$sem) = lvs
            }
            return(d)
        },

        ending_______________ = function() {}

    )
)

NCQDepthTimeline  <- setRefClass("NCQDepthTimeline",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path, timeinterval=c(0, 100000))
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpBarrier)
            
            plot_timeline(d, timeinterval)
        },
        plot_timeline = function(d, timeinterval)
        {
            d = melt(d, id=c('myexpname', 'timestamp', 'offset', 'size', 'operation', 'appname', 'testname'),
                     measure=c('pre_depth', 'post_depth'))

            d = arrange(d, timestamp, variable)
            d = subset(d, timestamp < 10^5)
            # d = head(d, 1000)

            # d = subset(d, timestamp > 5 & timestamp < 6)
            # d = subset(d, timestamp >= 50 & timestamp <= 52)
            d = subset(d, timestamp >= timeinterval[1] & timestamp < timeinterval[2])

            p = ggplot(d, aes(x=timestamp, y=value)) + 
                geom_line() +
                ylab('NCQ depth') +
                xlab('Timestamp') +
                expand_limits(y=0) +
                facet_grid(operation~appname) +
                # facet_grid(~appname) +
                theme_zplot()
            # print_plot(p)
            save_plot(p, 'ncq-timeline', save=T, w=20, h=4, ext='png')
        },
 
        ending_______________ = function() {}
    )
)


NCQDepthTimelineSnippet  <- setRefClass("NCQDepthTimelineSnippet",
    fields = list(expname='character'),
    contains = c('PlotWithCache'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = get_data(exp_rel_path, classname='ncqdepth', try_cache=T)
            
            plot_timeline(d)
        },
        get_raw_data = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpBarrier)
            return(d)
        },
        plot_timeline = function(d)
        {
            d = melt(d, id=c('myexpname', 'timestamp', 'offset', 'size', 'operation', 'appname', 'testname'),
                     measure=c('pre_depth', 'post_depth'))

            d = arrange(d, timestamp, variable)
            d = subset(d, timestamp < 10^5)

            # d = subset(d, timestamp > 5 & timestamp < 6)
            # d = subset(d, timestamp >= 50 & timestamp <= 52)
            # d = subset(d, timestamp >= 20 & timestamp < 20.1)

            d1 = subset(d, appname == 'rocksdb' & 
                        timestamp >= 20.5  & timestamp < 20.6 )
            d1 = transform(d1, timestamp = timestamp - min(timestamp))
            d2 = subset(d, appname == 'leveldb' & timestamp >= 20 & timestamp < 20.1)
            d2 = transform(d2, timestamp = timestamp - min(timestamp))
            d = rbind(d1, d2)

            d = transform(d, 
                appname = factor(appname, levels=c('leveldb', 'rocksdb')))

            p = ggplot(d, aes(x=timestamp, y=value)) + 
                geom_line(size=0.25) +
                ylab('NCQ depth') +
                xlab('Time (sec)') +
                expand_limits(y=0) +
                facet_grid(appname~., scales='free_x') +
                theme_zplot() +
                theme(plot.margin = unit(c(0.1, 0, 0, 0), 'cm')) +
                theme(strip.text.y = element_text(size=7))
            print_plot(p)
            save_plot(p, 'ncq-timeline', save=T, w=3.2, h=1.2, ext='pdf')
        },
        ending_______________ = function() {}
    )
)





c_____BANDWIDTH______________ <- function(){}

SubExpBandwidth <- setRefClass("SubExpBandwidth",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            apptimetxt = AppTimeTxt(
                  filepath=paste(subexppath, 'app_duration.txt', sep='/'))
            recjson <<- RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            duration = apptimetxt$load()
            written_size = recjson$get_ftlcounter_written_bytes() / MB

            return(list(
                    bw = written_size / duration,
                    duration = duration,
                    written_size = written_size,
                    myexpname = confjson$get_appmix_myexpname(),
                    appname = confjson$get_appmix_first_appname(),
                    n_insts = confjson$get_appmix_n_insts(),
                    fs_discard = confjson$get_appmix_fs_discard(),
                    filesystem = confjson$get_filesystem()
                    ))
        }
    )
)


Bandwidth <- setRefClass("Bandwidth",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_list_to_dataframe(exp_rel_path, SubExpBandwidth)

            d = cols_to_num(d, c('bw', 'n_insts'))
            d = subset(d, n_insts < 64)
            d$n_insts = sort_num_factor(d$n_insts)

            print(d)

            p = ggplot(d, aes(x=n_insts, y=bw, color=filesystem)) +
                geom_line(aes(group=filesystem), size=0.3) +
                # geom_point() +
                # scale_color_manual(values=get_fs_color_map()) +
                ylab('Write Bandwidth (MB/s)') +
                xlab('Num of Instances') +
                facet_grid(~appname) +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                expand_limits(y=0)
            print(p)
            save_plot(p, 'bandwidth', save=T, w=3.2, h=2.5)
        },
        ending_______________ = function() {}
    )
)

c_____BANDWIDTH_PAPER_____________ <- function(){}

paper_bw <- function()
{
    SubExpBandwidth <- setRefClass("SubExpBandwidth",
        fields = list(subexppath="character", confjson='ConfigJson', 
                      recjson='RecorderJson'),
        methods = list(
            run = function()
            {
                confjson <<- create_configjson(subexppath=subexppath)
                apptimetxt = AppTimeTxt(
                      filepath=paste(subexppath, 'app_duration.txt', sep='/'))
                recjson <<- RecorderJson(
                      filepath=paste(subexppath, 'recorder.json', sep='/'))

                duration = apptimetxt$load()
                written_size = recjson$get_ftlcounter_written_bytes() / MB

                return(list(
                        bw = written_size / duration,
                        duration = duration,
                        written_size = written_size,
                        myexpname = confjson$get_appmix_myexpname(),
                        appname = confjson$get_appmix_first_appname(),
                        n_insts = confjson$get_appmix_n_insts(),
                        fs_discard = confjson$get_appmix_fs_discard(),
                        filesystem = confjson$get_filesystem()
                        ))
            }
        )
    )


    Bandwidth <- setRefClass("Bandwidth",
        fields = list(expname='character'),
        methods = list(
            plot = function(exp_rel_path, exp_rel_path_varmail)
            {
                d.1 = aggregate_subexp_list_to_dataframe(exp_rel_path, SubExpBandwidth)
                d.1 = subset(d.1, !(appname == 'Varmail' & filesystem == 'f2fs'))
                d.1 = cols_to_num(d.1, c('duration', 'written_size'))

                d.varmail = aggregate_subexp_list_to_dataframe(exp_rel_path_varmail, SubExpBandwidth)
                d.varmail = cols_to_num(d.varmail, c('duration', 'written_size'))
                print(d.varmail)
                d.varmail$duration = 30
                d.varmail = transform(d.varmail, bw = written_size / duration)

                d.1 = cols_to_num(d.1, c('bw', 'duration', 'written_size'))
                d.varmail = cols_to_num(d.varmail, c('bw', 'duration', 'written_size'))
                d = rbind(d.1, d.varmail)

                d = cols_to_num(d, c('bw', 'n_insts'))
                d = subset(d, n_insts < 64)
                d$n_insts = sort_num_factor(d$n_insts)

                p = ggplot(d, aes(x=n_insts, y=bw, color=filesystem)) +
                    geom_line(aes(group=filesystem), size=0.3) +
                    # geom_point() +
                    scale_color_manual(values=get_fs_color_map()) +
                    ylab('Write Bandwidth (MB/s)') +
                    xlab('Num of Instances') +
                    facet_grid(~appname) +
                    theme_zplot() +
                    theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                    expand_limits(y=0)
                print(p)
                save_plot(p, 'bandwidth', save=T, w=3.2, h=2.5)
            },
            ending_______________ = function() {}
        )
    )


    bw = Bandwidth()
    bw$plot('all-fs-apps-16gb-3.noblkfiles',
              'varmail-f2fs-30sec'
              )
}



c_____BANDWIDTH_more_____________ <- function(){}


SubExpAppTimeOnly <- setRefClass("SubExpAppTimeOnly",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            print(subexppath)
            confjson <<- create_configjson(subexppath=subexppath)
            apptimetxt = AppTimeTxt(
                  filepath=paste(subexppath, 'app_duration.txt', sep='/'))

            duration = apptimetxt$load()

            return(list(
                    duration = duration,
                    myexpname = confjson$get_appmix_myexpname(),
                    appname = confjson$get_appmix_first_appname(),
                    n_insts = confjson$get_appmix_n_insts(),
                    fs_discard = confjson$get_appmix_fs_discard(),
                    filesystem = confjson$get_filesystem()
                    ))
        }
    )
)

SubExpAppSizeOnly <- setRefClass("SubExpAppSizeOnly",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            apptimetxt = AppTimeTxt(
                  filepath=paste(subexppath, 'app_duration.txt', sep='/'))
            recjson <<- RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            written_size = recjson$get_ftlcounter_written_bytes() / MB

            return(list(
                    written_size = written_size,
                    myexpname = confjson$get_appmix_myexpname(),
                    appname = confjson$get_appmix_first_appname(),
                    n_insts = confjson$get_appmix_n_insts(),
                    fs_discard = confjson$get_appmix_fs_discard(),
                    filesystem = confjson$get_filesystem()
                    ))
        }
    )
)




TwoPassBandwidth <- setRefClass("TwoPassBandwidth",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path_timeonly, exp_rel_path_sizeonly)
        {
            d.timeonly = aggregate_subexp_list_to_dataframe(exp_rel_path_timeonly, SubExpAppTimeOnly)
            d.sizeonly = aggregate_subexp_list_to_dataframe(exp_rel_path_sizeonly, SubExpAppSizeOnly)

            print(head(d.timeonly))
            print(head(d.sizeonly))

            d = merge(d.timeonly, d.sizeonly)
            
            plot_bw(d)

        },
        plot_bw = function(d)
        {
            d = cols_to_num(d, c('written_size', 'duration'))
            d = transform(d, bw = written_size / duration)
            d$n_insts = sort_num_factor(d$n_insts)

            print(d)

            p = ggplot(d, aes(x=n_insts, y=bw, color=filesystem)) +
                geom_line(aes(group=filesystem), size=0.3) +
                # geom_point() +
                scale_color_manual(values=get_fs_color_map()) +
                ylab('Write Bandwidth (MB/s)') +
                xlab('Num of Instances') +
                facet_grid(~appname) +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                expand_limits(y=0)
            print(p)
            save_plot(p, 'bandwidth', save=T, w=3.2, h=2.5)
        },
        ending_______________ = function() {}
    )
)


c_____NCQDEPTH_CDF_FOR_PAPER_____________ <- function(){}

SubExpNCQDepth <- setRefClass("SubExpNCQDepth",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)

            depth_timeline = NcqDepthTimelineTable(
                  filepath=paste(subexppath, 'ncq_depth_timeline.txt', sep='/'))
            d.depth = depth_timeline$load()

            d = d.depth

            d = transform(d, expname = confjson$get_expname())
            d = transform(d, myexpname = confjson$get_appmix_myexpname(),
                          n_insts=confjson$get_appmix_n_insts(),
                          filesystem=confjson$get_filesystem(),
                          appname=confjson$get_appmix_first_appname(),
                          fs_discard=confjson$get_appmix_fs_discard()
                          )

            return(d)
        },

        ending_______________ = function() {}

    )
)


NCQDepthCDFs  <- setRefClass("NCQDepthCDFs",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            # d = aggregate_subexp_dataframes(exp_rel_path, SubExpBarrier)
            # d = subset(d, action == 'D')
            
            # plot_depth_cdf(d)
            d.leveldb = get_leveldb()
            d.sqlite = get_sqlite()
            d.varmail = get_varmail()

            d = rbind(d.leveldb, d.sqlite, d.varmail)

            plot_depth_cdf(d)

        },

        get_leveldb = function()
        {
            # ext4
            d.ext4 = aggregate_subexp_dataframes('leveldb-10m-4inst', SubExpNCQDepth)
            d.ext4$fs_discard = TRUE

            # f2fs
            d.f2fs = aggregate_subexp_dataframes('leveldb-10m-f2fs-4inst', SubExpNCQDepth)
            d.f2fs$fs_discard = TRUE

            # xfs
            d.xfs = aggregate_subexp_dataframes('leveldb-xfs-4-inst-gogo', SubExpNCQDepth)
            d.xfs$fs_discard = TRUE

            d = rbind(d.ext4, d.f2fs, d.xfs)
            return(d)
        },
        get_sqlite = function()
        {
            # ext4
            d.ext4 = aggregate_subexp_dataframes('sqlite-ext4-4inst-go', SubExpNCQDepth)
            d.ext4$fs_discard = TRUE

            # f2fs
            d.f2fs = aggregate_subexp_dataframes(
                 'sqlite-f2fs-1-4-insts-discard-T-F/subexp--3733746757608966617-f2fs-09-23-22-50-41-5912988510877335738', 
                 SubExpNCQDepth)

            # xfs
            d.xfs = aggregate_subexp_dataframes(
                'sqlite-xfs-discard-nodiscard-1-4-inst/subexp--1277215920900524584-xfs-09-24-09-27-37--4114517400537133574', SubExpNCQDepth)

            d = rbind(d.ext4, d.f2fs, d.xfs)

            return(d)

        },
        get_varmail = function()
        {
            d = aggregate_subexp_dataframes("varmail-3fs-discard-T-F-1-4-fastfastext4", 
                    SubExpNCQDepth)

            d = subset(d, fs_discard == TRUE & n_insts == 4)
            return(d)
        },
 
        plot_depth_cdf = function(d)
        {
            # p = ggplot(d, aes(x=pre_depth, color=sem)) + 
                # stat_ecdf() +
                # facet_grid(myexpname~sem)
            # print_plot(p)

            d = subset(d, action == 'D') # we need both read and write for req scale
            d = subset(d, operation != 'discard')

            save(d, file='ncq-depth-data.rdata')

            p = ggplot(d, aes(x=pre_depth, color=filesystem)) + 
                stat_ecdf(size=0.3) +
                xlab('NCQ Depth') +
                ylab('Cumulative Density') +
                scale_color_manual(values=get_fs_color_map()) +
                facet_grid(~appname) +
                theme_zplot()
            print_plot(p)
            save_plot(p, 'ncq-depth-cdf', save=T, w=3.2, h=2.5)
        },
        ending_______________ = function() {}
    )
)





c_____Discard_count_BY_CACHE_____________ <- function(){}

plot_discard_count_by_my_cache <- function(d) 
{
    print(d)

    p = ggplot(d, aes(x=filesystem, y=count, fill=filesystem)) + 
        geom_bar(stat='identity', width=0.7) +
        xlab('File System') +
        ylab('Num of Discards') +
        # scale_x_continuous(limits=c(-1, xmax * 1.1)) +
        scale_fill_manual(values=get_fs_color_map()) +
        scale_y_log10(breaks=c(10, 1000, 100000),
                      labels=c('10', '1000', '100,000')
                      ) +
        facet_grid(~appname) +
        theme_zplot() +
        theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))

    print(p)
    save_plot(p, 'discard-count', save=T, w=3.2, h=2.5)
}

plot_discard_count <- function()
{
    text = "filesystem appname  count
ext4 LevelDB  19124
f2fs LevelDB   6248
xfs LevelDB  19901
ext4  Sqlite  47790
f2fs  Sqlite   2509
xfs  Sqlite  48008
ext4 Varmail 753380
f2fs Varmail   2605
xfs Varmail 482503"
    d = read.table(text=text, header=T)
    plot_discard_count_by_my_cache(d)
}


c_____Req_Size_CDF_By_CACHE_____________ <- function(){}

plot_req_size_cdf_by_cache <- function()
{
    load('reqsize-cdf-df-do-not-delete.rdata') 

    plot_req_size_cdf(d)
}

plot_req_size_cdf <- function(d)
{
    # d = subset(d, operation == 'write' & action == 'D')
    d = subset(d, action == 'D') # we need both read and write for req scale
    d = subset(d, operation != 'discard')

    p = ggplot(d, aes(x=size, color=filesystem)) + 
        stat_ecdf(size=0.3) +
        # coord_cartesian(xlim=c(0, 2000)) +
        xlab('Write Request Size (KB)') +
        ylab('Cumulative Density') +
        scale_color_manual(values=get_fs_color_map()) +
        facet_grid(~appname) +
        theme_zplot() +
        theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
    print_plot(p)
    save_plot(p, 'req-size-ecdf', save=T, w=3.2, h=2.5)
}



c_____NCQ_Depth_CDF_By_CACHE_____________ <- function(){}

plot_ncq_depth_cdf_by_cache <- function()
{
    load('ncq-depth-data-do-not-delete.rdata')
    plot_depth_cdf(d)
}


plot_depth_cdf = function(d)
{
    # p = ggplot(d, aes(x=pre_depth, color=sem)) + 
        # stat_ecdf() +
        # facet_grid(myexpname~sem)
    # print_plot(p)

    d = subset(d, action == 'D') # we need both read and write for req scale
    d = subset(d, operation != 'discard')

    p = ggplot(d, aes(x=pre_depth, color=filesystem)) + 
        stat_ecdf(size=0.3) +
        xlab('NCQ Depth') +
        ylab('Cumulative Density') +
        scale_color_manual(values=get_fs_color_map()) +
        facet_grid(~appname) +
        theme_zplot()
    print_plot(p)
    save_plot(p, 'ncq-depth-cdf', save=T, w=3.2, h=2.5)
}


batch_plot <- function(exp_rel_path)
{
    # ftime = FsyncTimeline()
    # ftime$plot(exp_rel_path)

    # bw = Bandwidth()
    # bw$plot(exp_rel_path)

    # typestats = ReqDataTypeStats()
    # typestats$plot(exp_rel_path)

    # sizebysem = SizeBySem()
    # sizebysem$plot(exp_rel_path)

    # nprocs = NumProcs()
    # nprocs$plot(exp_rel_path)

    ######### For exploring
    # sizecdf = BlkReqSizeCDF_EXPLORE()
    # sizecdf$plot(exp_rel_path)

    # exp = NCQDepthTimeline()
    # exp$plot(exp_rel_path)

    ######### For paper
    # size_cdf = BlkReqSizeCDF_4_PAPER()
    # size_cdf$plot(exp_rel_path)

    # depth_cdf = NCQDepthCDF_4_Paper()
    # depth_cdf$plot(exp_rel_path)

    # discard = DiscardCount_4_PAPER()
    # discard$plot(exp_rel_path)


    ######## NEW PLOTs
    req_size = Req_Size_Box()
    req_size$plot(exp_rel_path)

    depth = NCQ_Depth_Box()
    depth$plot(exp_rel_path)

    discard_ratio = Discard_Ratio()
    discard_ratio$plot(exp_rel_path)

    compare_rocksdb_leveldb_ncq_timeline()

    ######## different comparison
    # req_size = Req_Size_Box_comppattern()
    # req_size$plot(exp_rel_path)

    # depth = NCQ_Depth_Box_comppattern()
    # depth$plot(exp_rel_path)

    # discard_ratio = Discard_Ratio_comppattern()
    # discard_ratio$plot(exp_rel_path)

    ######## for explore
    # avgsize = Avg_Req_Size()
    # avgsize$plot(exp_rel_path)

    # ncq_median = NCQ_Depth_Median_Table()
    # ncq_median$plot(exp_rel_path)
}


compare_rocksdb_leveldb_ncq_timeline <- function()
{
    exp_rel_path = c("rocksdb-reqscale/subexp--2492029546453855134-ext4-10-05-22-30-28--5872463825357626395",
                    "leveldb-reqscale-001/subexp-1607208280200404504-ext4-10-06-09-49-49--3142468957932722792")
    exp = NCQDepthTimelineSnippet()
    print('TTTTTTTTTimeline')
    exp$plot(exp_rel_path)
    # exp$plot(exp_rel_path)
}



main <- function()
{
    batch_plots <<- FALSE

    # batch_plot('rocksdb_reqscale_r_mix_allfs') 

    # batch_plot('rocksdbw') 
    # batch_plot('rockdb-optimized') 
    # batch_plot('2gmb') 
    # batch_plot('rocksdb-w-rand') 
    # batch_plot('rocksdb-seq-w') 
    # batch_plot('3million-10times') 
    # batch_plot('1writebuffer') 
    # batch_plot('compactionatend') 
    # batch_plot('1threadcompactflush') 
    # batch_plot('32threadcompaction') 
    # batch_plot('no-auto-compaction') 
    # batch_plot('32-compaction-threads') 
    # batch_plot('1-compaction-thread') 
    # batch_plot('10m-3times-1thread') 
    # batch_plot('10m-3times-32threads') 
    # batch_plot('10m-1time-32threads') 
    # batch_plot('10m-1time-1thread') 
    # batch_plot('2gb-10m-3times-32threads')
    # batch_plot('2gb-10m-3times')
    # batch_plot('64mbfile')
    # batch_plot('rocksdb-reqscale/subexp--1021956456338881965-ext4-10-05-22-22-48-6570092437004215288')
    # batch_plot('ones')

    # batch_plot('rocksdb-reqscale')
    # batch_plot('leveldb-reqscale-001')
    # batch_plot('sqliteWAL-reqscale-001')
    # batch_plot('sqliteRB-reqscale-001')
    # batch_plot('varmail-reqscale-001')
    # batch_plot('rocksdb-longer-for-gc-sorttrace2')
    # return()

    ##################################
    ##################################
    ########### PAPER ZONE ###########
    ##################################
    ##################################
    batch_plot(c(
                 # all have some that do not run long enough (e.g. 400MB)
                 # except varmail
                 'rocksdb-reqscale',
                 'leveldb-reqscale-001',
                 'sqliterb-reqscale-240000-insertions-4',
                 'sqlitewal-reqscale-240000-inserts-3',
                 'varmail-reqscale-002'

                 # The following are longer than the one above
                 # They all should trigger GC 
                 # 'leveldb-longer-for-gc-sorttrace',
                 # 'rocksdb-longer-for-gc-sorttrace2',
                 # 'sqlitewal-longer-for-gc-sorttrace',
                 # 'sqliterb-longer-for-gc-sorttrace',
                 # 'varmail-reqscale-002'
                 ))



    #################################################

    print_global_plots(save_to_file=F, w=20, h=20)

}
main()











