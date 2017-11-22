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
    THISFILE     ='doraemon/analyze-wearleveling.r'
    setwd(WORKDIRECTORY)
    source(THISFILE)
}

c______LBA_____________ <- function(){}

SubExpErasureDist <- setRefClass("SubExpErasureDist",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            recjson <<- RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            snapshots = recjson$get_erasure_count_snapshots()
            d = organize_data(snapshots)

            d = transform(d, distribution = get_distribution())
            d = transform(d, blocksize = confjson$get_block_size())
            d = transform(d, ftl_type = confjson$get_ftl_type())
            d = transform(d, filesystem = get_fs())

            d = sample_snapshots(d, 10)
            snapshot_ids = sort(unique(d$snapshot_id))
            new_ids = seq_along(snapshot_ids)
            trans_d = data.frame(snapshot_id=snapshot_ids, new_ids)
            d = merge(d, trans_d)
            d = transform(d, snapshot_id = new_ids)

            return(d)
        },
        get_distribution = function()
        {
            dist = confjson$get_access_distribution()
            appnames = confjson$get_appmix_appnames()
            if (is.null(dist))
                return(appnames)
            else
                return(dist)
        },
        get_fs = function()
        {
            fs = confjson$get_filesystem()
            if (is.null(fs)) 
                return('NULL')
            else
                return(fs)
        },
        organize_data = function(snapshots)
        {
            d = melt(as.matrix(snapshots))
            d = as.data.frame(d)
            names(d) = c('snapshot_id', 'erasure_cnt', 'block_cnt')
            d = subset(d, is.na(block_cnt) == FALSE)

            return(d)
        },
        sample_snapshots = function(d, n)
        {
            max_snapshot_id = max(d$snapshot_id)
            print('max_snapshot_id')
            print(max_snapshot_id)
            sel = as.integer(seq(1, max_snapshot_id, length.out=n))
            d = subset(d, snapshot_id %in% sel)
            return(d)
        }
    )
)

set_line_points_for_a_snapshot <- function(d) {
    d = arrange(d, desc(erasure_cnt))
    d = transform(d, seg_end = cumsum(block_cnt))
    d = transform(d, seg_start = seg_end - block_cnt)
    d = melt(d, 
         # id=c('valid_ratio', 'segment_bytes', 'blocksize', 'filesystem'), 
         measure = c('seg_start', 'seg_end'), value.name = 'blocknum')
    d = arrange(d, desc(erasure_cnt))

    return(d)
}

AggregatorErasureDist <- setRefClass("AggregatorErasureDist",
    fields = list(expname='character'),
    methods = list(
        plot_block_erasure_counts = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(
               exp_rel_path, SubExpErasureDist)

            d = fast_organize_(d)

            d = transform(d, block_location = (as.numeric(blocknum)/MB) * as.numeric(blocksize))

            # d = subset(d, distribution != 'hotcold')

            d = transform(d, ftl_type = revalue(ftl_type, c('nkftl2'='Hybrid', 'dftldes'='Page-level')))

            p = ggplot(d, aes(x=block_location, y=erasure_cnt, 
                              color=factor(snapshot_id))) +
                geom_line() +
                scale_color_grey(start=0.9, end=0) +
                facet_grid(ftl_type~distribution) +
                ylab('Erasure Count') +
                xlab('Block (sorted by erasure count)') +
                theme_zplot() +
                theme(legend.position="none") +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            print(p)
            save_plot(p, 'erasure-count-snapshots', save=T, h=3)
        },

        plot_for_app_fs = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(
               exp_rel_path, SubExpErasureDist)

            d = fast_organize_(d)

            d = transform(d, block_location = (as.numeric(blocknum)/MB) * as.numeric(blocksize))

            # d = subset(d, distribution != 'hotcold')

            d = transform(d, ftl_type = revalue(ftl_type, c('nkftl2'='Hybrid', 'dftldes'='Page-level')))

            p = ggplot(d, aes(x=block_location, y=erasure_cnt, 
                              color=factor(snapshot_id))) +
                geom_line() +
                scale_color_grey(start=0.9, end=0) +
                facet_grid(filesystem~distribution) +
                ylab('Erasure Count') +
                xlab('Block (sorted by erasure count)') +
                theme_zplot() +
                theme(legend.position="none") +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            print(p)
            save_plot(p, 'erasure-count-snapshots', save=T, h=3)
        },

        fast_organize_ = function(d)
        {
            d = ddply(d, .(distribution, snapshot_id, ftl_type, filesystem), 
                      set_line_points_for_a_snapshot)
            return(d)
        }
    )
)

c______LPN_COUNT_____________ <- function(){}

SubExpLpnCount <- setRefClass("SubExpLpnCount",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            lpncount <- LpnCount(
                  filepath=paste(subexppath, 'lpn.count', sep='/'))

            d = lpncount$load()

            d = subset(d, write > 0)
            # d = transform(d, count = write + discard)
            d = transform(d, count = write)
            d = arrange(d, desc(count))

            d = transform(d, lpn_pos = seq_along(count))
            d = transform(d, lpn_size = lpn_pos * 2*KB / MB)
            d = transform(d, lpn_ratio = lpn_size / max(lpn_size))
            d = transform(d, benchname = confjson$get_appmix_appnames())
            d = transform(d, filesystem = confjson$get_filesystem())

            # plot_individual(d)
            # a = readline()
            # if (a == 'q') 
                # stop()
            # print('WARNING: debugging............here')
            # d = head(d, 80)

            return(d)
        },
        plot_individual = function(d)
        {
            p = ggplot(d, aes(x=lpn_size, y=write))+
                # geom_point()
                geom_line() +
                facet_grid(benchname~filesystem) +
                theme_zplot()
            print(p)

        }
    )
)

LogicalSpaceHist <- setRefClass("LogicalSpaceHist",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpLpnCount)

            d = cols_to_num(d, c('read', 'write', 'discard', 'count'))
            d = transform(d, filesystem = factor(filesystem, 
                     levels=c('btrfs', 'ext4', 'f2fs', 'xfs')))
            p = ggplot(d, aes(x=lpn*2*KB/MB, y=count))+
                geom_point() +
                coord_cartesian(ylim = c(0, 1000)) +
                xlab('Used Space') 
            print(p)
        }
    )
)



LogicalSpaceFreqPlot <- setRefClass("LogicalSpaceFreqPlot",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpLpnCount)

            d = cols_to_num(d, c('read', 'write', 'discard', 'count'))
            d = transform(d, filesystem = factor(filesystem, 
                     levels=c('btrfs', 'ext4', 'f2fs', 'xfs')))

            dd = get_max_coord(d)
            print(dd)
            
            d$benchname = factor(d$benchname, levels = sort(levels(d$benchname)))

            d = transform(d, interval_id = findInterval(lpn_ratio, seq(0, 1, 0.001)))
            d = ddply(d, .(interval_id, filesystem, benchname), head, 1)

            # save(d, dd, file='lba-write-freq.rdata')

            p = ggplot(d, aes(x=lpn_ratio, y=count, color=filesystem))+
                geom_line() +
                geom_point(data=dd, aes(x=lpn_ratio, y=count, 
                                        color=filesystem, shape=filesystem), 
                           size=2) +
                facet_grid(~benchname) +
                scale_color_grey(start=0.7, end=0) +
                scale_x_continuous(breaks=c(0, 0.5, 1),
                                   labels=as.character(c(0, 0.5, 1)))+
                xlab('Used Logical Space (normalized)') +
                scale_y_log10() +
                theme_zplot()
            print(p)
            save_plot(p, 'lba-write-freq', save=T, w=3.2, h=2.5)
        },
        get_max_coord = function(d)
        {
            dd = ddply(d, .(benchname, filesystem), subset, lpn_ratio == min(lpn_ratio)) 

            return(dd)
        }
    )
)


LogicalSpaceFreqPlotUnSorted <- setRefClass("LogicalSpaceFreqPlotUnSorted",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpLpnCount)

            d = cols_to_num(d, c('read', 'write', 'discard', 'count'))
            d = transform(d, filesystem = factor(filesystem, 
                     levels=c('btrfs', 'ext4', 'f2fs', 'xfs')))
            d = transform(d, offset = lpn * 2 * KB/MB)

            p = ggplot(d, aes(x=offset, y=count))+
                geom_point() +
                # geom_line() +
                facet_grid(benchname~filesystem) +
                scale_color_manual(
                   values=get_color_map2(c('btrfs', 'ext4', 'f2fs', 'xfs'))) +
                scale_y_log10() +
                # xlab('Logical Pages (MB)') +
                xlab('Used Space') +
                theme_zplot()
            print(p)
        }
    )
)


c______TRAFFIC_____________ <- function(){}

SubExpRawCount <- setRefClass("SubExpRawCount",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            lpncount <- LpnCount(
                  filepath=paste(subexppath, 'lpn.count', sep='/'))

            d = lpncount$load()

            d = subset(d, write > 0)
            # d = transform(d, count = write + discard)
            d = transform(d, count = write)

            d = transform(d, benchname = confjson$get_appmix_appnames())
            d = transform(d, filesystem = confjson$get_filesystem())
            d = transform(d, subexpname = confjson$get_subexpname())

            return(d)
        }
    )
)



TrafficSize <- setRefClass("TrafficSize",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpRawCount)

            d = cols_to_num(d, c('read', 'write', 'discard', 'count'))
            d = transform(d, filesystem = factor(filesystem, 
                     levels=c('btrfs', 'ext4', 'f2fs', 'xfs')))

            d = aggregate(write~filesystem + benchname + subexpname, data=d, sum)

            d = transform(d, write_bytes = write * 2 * KB/GB)

            print(d)
        }
    )
)


c______LPN_SEMANTICS_____________ <- function(){}

SubExpLpnSem <- setRefClass("SubExpLpnSem",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            lpncount <- LpnCount(
                  filepath=paste(subexppath, 'lpn.count', sep='/'))
            lpnsem <- LpnSem(
                  filepath=paste(subexppath, 'lpn_sem.out', sep='/'))

            d.sem = lpnsem$load()
            d.sem = replace_sem(d.sem)

            d = lpncount$load()

            # print('WARNING: DEBUGGING')
            # d = head(d, 1000)
            # d.sem = head(d.sem, 1000)


            d = subset(d, write > 0)
            # d = transform(d, count = write + discard)
            d = transform(d, count = write)
            d = arrange(d, desc(count))
            d = transform(d, lpn_pos = seq_along(count))
            d = transform(d, lpn_size = lpn_pos * 2*KB / MB)
            d = transform(d, lpn_ratio = lpn_size / max(lpn_size))
            d = transform(d, benchname = confjson$get_appmix_appnames())
            d = transform(d, filesystem = confjson$get_filesystem())

            d = merge(d, d.sem)
            d = arrange(d, desc(count))


            d.stats1 = get_top_or_bottom_sem_average(d, 0.10, 'top')
            d.stats2 = get_top_or_bottom_sem_average(d, 0.10, 'bottom')

            d.stats = rbind(d.stats1, d.stats2)
            return(d.stats)
        },
        get_top_or_bottom_sem_average = function(d, ratio, position)
        {
            d = get_top_or_bottom(d, ratio, position)

            d = transform(d, n_pages = 1)
            # number of pages of each sem type
            d.n_pages = aggregate(n_pages~sem + benchname + filesystem, data=d, sum)
            # total number of accesses of each sem type
            d.n_access = aggregate(count~sem + benchname + filesystem, data=d, sum)
            d.stats = merge(d.n_pages, d.n_access)
            d.stats = transform(d.stats, avg_acc_count = count / n_pages)

            d.stats = transform(d.stats, ratio = ratio, position = position)

            return(d.stats)
        },
        get_top_or_bottom = function(d, ratio, position)
        {
            n = nrow(d)
            n_picked_rows = as.integer(n * ratio)

            if (position == 'top')
                d = head(d, n_picked_rows)
            else if (position == 'bottom')
                d = tail(d, n_picked_rows)
            else
                stop('wrong position')

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
        }
    )
)

LpnSemStats <- setRefClass("LpnSemStats",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path, need_position)
        {
            stopifnot(length(need_position) == 1)

            d = aggregate_subexp_dataframes(exp_rel_path, SubExpLpnSem)

            d = subset(d, position %in% need_position)

            d$filesystem = factor(d$filesystem, levels = c('ext4', 'f2fs', 'xfs'))
            d$benchname = factor(d$benchname, levels = sort(levels(d$benchname)))

            print(d)
              # sem benchname filesystem n_pages  count avg_acc_count ratio position
# 1     Data-region    Sqlite        xfs       8 681822    85227.7500  0.01      top
# 2     Data-region   LevelDB        xfs       9    414       46.0000  0.01      top
# 3   FreeBlockInfo   Varmail        xfs       4   2082      520.5000  0.01      top
# 4 FreeSpTree1Root   Varmail        xfs       3   1828      609.3333  0.01      top
# 5 FreeSpTree2Root   Varmail        xfs       2   1572      786.0000  0.01      top

            p = ggplot(d, aes(x=sem, y=avg_acc_count)) +
                geom_bar(stat='identity', color='black', width=0.6) +
                facet_grid(benchname~filesystem, drop=T, space='free', scale='free') +
                scale_y_log10() +
                theme_zplot() +
                theme(axis.title.x = element_blank()) +
                theme(panel.margin.x = unit(0.7, "lines")) +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            print(p)
            save_plot(p, paste('sem-average', need_position[1], sep='-'), save=T, w=3.2, h=3.2)
        }
    )
)


LpnSemTable <- setRefClass("LpnSemTable",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpLpnSem)

            d$filesystem = factor(d$filesystem, levels = c('ext4', 'f2fs', 'xfs'))
            d$benchname = factor(d$benchname, levels = sort(levels(d$benchname)))

            print(d)
              # sem benchname filesystem n_pages  count avg_acc_count ratio position
# 1     Data-region    Sqlite        xfs       8 681822    85227.7500  0.01      top
# 2     Data-region   LevelDB        xfs       9    414       46.0000  0.01      top
# 3   FreeBlockInfo   Varmail        xfs       4   2082      520.5000  0.01      top
# 4 FreeSpTree1Root   Varmail        xfs       3   1828      609.3333  0.01      top
# 5 FreeSpTree2Root   Varmail        xfs       2   1572      786.0000  0.01      top

            d = transform(d, position = factor(position, levels=c('bottom', 'top')))

            d = transform(d, sem = factor(sem, levels=sort(levels(sem))))

            p = ggplot(d, aes(x=sem, y=position)) +
                geom_point(size=3, shape='X', color='black') +
                facet_grid(benchname~filesystem, drop=T, space='free', scale='free') +
                theme_zplot() +
                theme(axis.title.x = element_blank()) +
                theme(axis.title.y = element_blank()) +
                theme(strip.text.y = element_text(angle=0)) +
                theme(panel.margin.x = unit(0.2, "lines")) +
                theme(panel.margin.y = unit(0.3, "lines")) +
                theme(legend.position="none") +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            print(p)
            save_plot(p, 'sem-table', save=T, w=3.2, h=2.5)
        }
    )
)



c______LPN_SEMANTICS_EXPLORE_____________ <- function(){}

SubExpLpnSemExp <- setRefClass("SubExpLpnSemExp",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            lpncount <- LpnCount(
                  filepath=paste(subexppath, 'lpn.count', sep='/'))
            lpnsem <- LpnSem(
                  filepath=paste(subexppath, 'lpn_sem.out', sep='/'))

            d.sem = lpnsem$load()
            # d.sem = replace_sem(d.sem)

            d = lpncount$load()

            # print('WARNING: DEBUGGING')
            # d = head(d, 1000)
            # d.sem = head(d.sem, 1000)


            d = subset(d, write > 0)
            # d = transform(d, count = write + discard)
            d = transform(d, count = write)
            d = arrange(d, desc(count))
            d = transform(d, lpn_pos = seq_along(count))
            d = transform(d, lpn_size = lpn_pos * 2*KB / MB)
            d = transform(d, lpn_ratio = lpn_size / max(lpn_size))
            d = transform(d, benchname = confjson$get_appmix_appnames())
            d = transform(d, filesystem = confjson$get_filesystem())

            d = merge(d, d.sem)
            d = arrange(d, desc(count))


            d.stats1 = get_top_or_bottom(d, 0.01, 'top')
            d.stats2 = get_top_or_bottom(d, 0.01, 'bottom')

            d.stats = rbind(d.stats1, d.stats2)

            return(d.stats)
        },
        get_top_or_bottom_sem_average = function(d, ratio, position)
        {
            d = get_top_or_bottom(d, ratio, position)

            d = transform(d, n_pages = 1)
            # number of pages of each sem type
            d.n_pages = aggregate(n_pages~sem + benchname + filesystem, data=d, sum)
            # total number of accesses of each sem type
            d.n_access = aggregate(count~sem + benchname + filesystem, data=d, sum)
            d.stats = merge(d.n_pages, d.n_access)
            d.stats = transform(d.stats, avg_acc_count = count / n_pages)

            d.stats = transform(d.stats, ratio = ratio, position = position)

            return(d.stats)
        },
        get_top_or_bottom = function(d, ratio, position)
        {
            n = nrow(d)
            n_picked_rows = as.integer(n * ratio)

            if (position == 'top')
                d = head(d, n_picked_rows)
            else if (position == 'bottom')
                d = tail(d, n_picked_rows)
            else
                stop('wrong position')

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
        }
    )
)

LpnSemStatsExp <- setRefClass("LpnSemStatsExp",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpLpnSemExp)
            print(d)
        }
    )
)




c______WEAR_LEVELING_COST_____________ <- function(){}

SubExpCost <- setRefClass("SubExpCost",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            recjson <<- RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            pagesize = confjson$get_page_size()

            l = list(
                moved_bytes=get_page_moves() * pagesize,
                dist=get_distribution(),
                do_leveling=confjson$get_do_wearleveling(),
                traffic_bytes=confjson$get_user_traffic_size(),
                end=8848
                )
            return(l)
        },
        get_page_moves = function()
        {
            ftl_type = confjson$get_ftl_type()
            if (ftl_type == 'nkftl2') {
                page_moves = recjson$get_nkftl_page_moves()
            } else if (ftl_type == 'dftldes') {
                page_moves = recjson$get_dftl_user_page_moves() + 
                        recjson$get_dftl_trans_page_moves()
            }
            return(page_moves)
        },
        get_distribution = function()
        {
            dist = confjson$get_access_distribution()
            appnames = confjson$get_appmix_appnames()
            if (is.null(dist))
                return(appnames)
            else
                return(dist)
        }
    )
)

CostPlot <- setRefClass("CostPlot",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_list_to_dataframe(exp_rel_path, SubExpCost)

            d = cols_to_num(d, c('moved_bytes', 'traffic_bytes'))
            d = transform(d, wa = moved_bytes / traffic_bytes)
            print(d)
        }
    )
)

c______zipf_dist_____________ <- function(){}


plot_zipf <- function()
{
    d = read.table('/Users/junhe/datahouse/localresults/zipf2/zipf.data', header = T, sep = ';')

    d = subset(d, alpha %in% c(0.1, 0.5, 1))

    d = table(d[, c('znum', 'alpha')])
    d = melt(d, id=c('znum'))
    d = plyr::rename(d, c('value'='count'))
    d = ddply(d, .(alpha), arrange, desc(count))
    d = ddply(d, .(alpha), transform, count_id = seq_along(count)/length(count))

    d = transform(d, alpha = factor(alpha))

    p = ggplot(d, aes(x=count_id, y=count, color=alpha)) +
        geom_line() +
        scale_color_grey(start=0, guide = guide_legend(title = "alpha")) +
        scale_y_log10() +
        xlab('Logical Space') +
        ylab('Update Count') +
        # facet_grid(~alpha) +
        theme_zplot() +
        theme(legend.position='top', legend.title=element_text())
    print(p)

    save_plot(p, 'zipf-dist', save=T, w=3.2, h=2.5)
}


c______LPN_COUNT_NEW_____________ <- function(){}

SubExpLpnCountNew <- setRefClass("SubExpLpnCountNew",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            lpncount <- LpnCount(
                  filepath=paste(subexppath, 'lpn.count', sep='/'))

            d = lpncount$load()

            d = subset(d, write > 0)
            # d = transform(d, count = write + discard)
            d = transform(d, count = write)
            d = arrange(d, desc(count))

            d = transform(d, lpn_pos = seq_along(count))
            d = transform(d, lpn_size = lpn_pos * 2*KB / MB)
            d = transform(d, lpn_ratio = lpn_size / max(lpn_size))
            d = transform(d, benchname = confjson$get_appmix_appnames())
            d = transform(d, filesystem = confjson$get_filesystem(),
                             testname=confjson$get_testname(),
                             rw=confjson$get_testname_rw(),
                             pattern=confjson$get_testname_pattern(),
                             appname=confjson$get_testname_appname(),
                             rule=confjson$get_testname_rulename()
                          )

            d = sample_down(d)

            return(d)
        },
        sample_down = function(d)
        {
            d = cols_to_num(d, c('read', 'write', 'discard', 'count'))
            d = transform(d, interval_id = findInterval(lpn_ratio, seq(0, 1, 0.001)))
            d = ddply(d, .(interval_id), head, 1)

            return(d)
        }
    )
)

LogicalSpaceFreqPlotNew <- setRefClass("LogicalSpaceFreqPlotNew",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = get_data(exp_rel_path)
            dd = get_max_coord(d)

            d = shorten_appnames(d)

            p = ggplot(d, aes(x=lpn_ratio, y=count, color=filesystem))+
                geom_line(aes(linetype=filesystem, size=filesystem)) +
                # geom_point(data=dd, aes(x=lpn_ratio, y=count, 
                                        # color=filesystem, shape=filesystem), 
                           # size=2) +
                facet_grid(pattern~appname) +
                scale_color_manual(values=get_fs_color_map()) +
                scale_linetype_manual(values=get_fs_linetype_map()) +
                scale_size_manual(values=get_fs_size_map()) +
                scale_x_continuous(breaks=c(0, 0.5, 1),
                                   labels=as.character(c(0, 0.5, 1)))+
                xlab('Used Logical Space (normalized)') +
                ylab('Count') +
                scale_y_log10() +
                theme_zplot() +
                theme(strip.text.x = element_text(size=7)) +
                theme(legend.position=c(0.7, 0.96), 
                      legend.direction="horizontal",
                      legend.title=element_blank(),
                      legend.background=element_blank()
                      )
            print(p)
            save_plot(p, 'lba-write-freq', save=T, w=3.2, h=2)
        },
        get_data = function(exp_rel_path)
        {
            my_cache_path = cache_path(exp_rel_path)
            if (cache_exists(exp_rel_path))  {
                print('Using cache')
                print(my_cache_path)
                load(my_cache_path)
            } else {
                print('No cache')
                d = get_data_from_raw(exp_rel_path)
                print(my_cache_path)
                save(d, file=my_cache_path)
            }
            return(d)
        },
        get_data_from_raw = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpLpnCountNew)

            d = transform(d, filesystem = factor(filesystem, 
                     levels=c('ext4', 'f2fs', 'xfs')))
            d = rename_pattern_levels(d)

            return(d)
        },
        cache_path = function(exp_rel_path)
        {
            exp_rel_path = gsub('/', '', exp_rel_path)
            long_path = paste(exp_rel_path, collapse='-')
            cache_path = paste(digest(long_path), 'freqplot', 'rdata', sep='.')
            return(cache_path)
        },
        cache_exists = function(exp_rel_path)
        {
            path = cache_path(exp_rel_path)
            return( file.exists(path) )
        },
        get_max_coord = function(d)
        {
            dd = ddply(d, .(testname, filesystem), subset, lpn_ratio == min(lpn_ratio)) 

            return(dd)
        }
    )
)





c______MAIN_____________ <- function(){}


paper_plot <- function()
{
    agg_erasure_dist = AggregatorErasureDist()
    agg_erasure_dist$plot_block_erasure_counts(
                           c('lbawear-dftl-uniform-gogo', 
                           'lbawear-dftl-zipf-gogo',
                           'lbawear-nkftl-2-dist'
                           ))

    plot_zipf()

    # the density of the logical space, sorted by access count
    logical_freq = LogicalSpaceFreqPlot()
    logical_freq$plot(
                      c(
                        'ext4-apps-lpn-count-paddingbugfixed',
                        'f2fs-correct-classification-long',
                        'xfs-3app-with-filerange', 
                        'varmail-6millions-ops-all-3-fses'
                        ))

    semplot = LpnSemStats()
    semplot$plot(c('xfs-3app-with-filerange',
                   'f2fs-correct-classification-long',
                   'ext4-apps-lpn-count-paddingbugfixed',
                   'varmail-6millions-ops-all-3-fses'
                   ),
                 c('top')
                 )

    semplot$plot(c('xfs-3app-with-filerange',
                   'f2fs-correct-classification-long',
                   'ext4-apps-lpn-count-paddingbugfixed',
                   'varmail-6millions-ops-all-3-fses'
                   ),
                 c('bottom')
                 )

    # Traffic size
    traffic = TrafficSize()
    traffic$plot(c(
                   'xfs-3app-with-filerange',
                   'f2fs-correct-classification-long',
                   'ext4-apps-lpn-count-paddingbugfixed',
                   'varmail-6millions-ops-all-3-fses'
                   ))

    # tablel of top and bottom 1% data type
    semtable = LpnSemTable()
    semtable$plot(c(
                   'xfs-3app-with-filerange',
                   'f2fs-correct-classification-long',
                   'ext4-apps-lpn-count-paddingbugfixed',
                   'varmail-6millions-ops-all-3-fses'
                   )
                 )


}


plot_lba_freq_by_cache <- function()
{
    load('lba-write-freq-do-not-delete.rdata')

    p = ggplot(d, aes(x=lpn_ratio, y=count, color=filesystem))+
        geom_line(size=0.4) +
        geom_point(data=dd, aes(x=lpn_ratio, y=count, 
                                color=filesystem, shape=filesystem), 
                   size=2) +
        facet_grid(~benchname) +
        scale_color_manual(values=get_fs_color_map())+
        scale_x_continuous(breaks=c(0, 0.5, 1),
                           labels=as.character(c(0, 0.5, 1)))+
        xlab('Used Logical Space (normalized)') +
        scale_y_log10() +
        theme_zplot()
    print(p)
    save_plot(p, 'lba-write-freq', save=T, w=3.2, h=2.5)
}



# >>>>>>>>>> Good data <<<<<<<<<<<
# 'ext4-apps-lpn-count-paddingbugfixed' 
#    every body good long run
#    varmail deleted, because it is not consistent with others
# 'f2fs-correct-classification-long' # data for the paper
#    varmail deleted, because it is not consistent with others
# 'xfs-3app-with-filerange'
#    varmail deleted, because it is not consistent with others
# 'varmail-6millions-ops-all-3-fses'
#    This runs varmail on all 3 file systems with the same 
#    ops



# >>>>>>>>>>> 100 GB
# datalife-100gb-f2fs-xfs
# datalife-100gb-ext4

# >>>>>>>>> seqlite min=150GB
# sqlite-24millions"
# 

# Write endurance of DC 3500 480GB
#  > 275*TB/(480*GB)
# [1] 586.6667

plot_100gb <- function()
{
    # traffic = TrafficSize()
    # traffic$plot(c(
                    # 'datalife-100gb-f2fs-xfs',
                    # 'datalife-100gb-ext4'
                   # ))


    logical_freq = LogicalSpaceFreqPlot()
    logical_freq$plot(
                      c(
                    'datalife-100gb-f2fs-xfs',
                    'datalife-100gb-ext4'
                        ))

}

main <- function()
{

    ################# PAPER PLOT #################
    logical_freq = LogicalSpaceFreqPlotNew()
    logical_freq$plot(c(
        # # this expname wear-leveling-allapps has no sqliteRB 
        # # or sqliteWAL now, I deleted them because they are misconfigured.
        # 'wear-leveling-allapps', 
        # 'sqlitewal-20m-makeup',
        # 'sqliterb-wear-20m-makeup4'

        'wear-leveling-allapps', 
        'sqlitewal-wear-seq100m', # WAL has 100 million insertions
        'sqliterb-wear-seq100m-11'
        ))

    return()


    semplot = LpnSemStats()
    semplot$plot(c(
        # 'wear-all-apps-by-celery3/subexp--3608896358734512622-ext4-10-12-22-10-44-7673791418511741498' # ext4 sqliteWAL_wearlevel_w_seq
        # sqliteWAL_wearlevel_w_rand, XFS
        # 'wear-all-apps-by-celery3/subexp-8638951636023146841-xfs-10-12-22-10-00--9011507884259744894'
        # 
        # sqliteWAL_wearlevel_w_rand, F2FS
        # 'wear-all-apps-by-celery3/subexp--2114287361282762076-f2fs-10-12-22-10-09-9165379581356720654'
        # varmail_wearlevel_w_large, XFS
        # 'wear-all-apps-by-celery3/subexp-3538230439051010162-xfs-10-12-21-59-21-812972423172081384'
        # rocksdb, seq, ext4
        # 'wear-all-apps-by-celery3/subexp-8394194217604040906-ext4-10-12-21-59-21-7954352482927572971'
        # varmail, ext4, large set
        # 'wear-all-apps-by-celery3/subexp--9146884560846585089-ext4-10-12-21-59-21--365354425960301322'
        # sqliteWAL, F2fS, rand
        # 'wear-all-apps-by-celery3/subexp--2114287361282762076-f2fs-10-12-22-10-09-9165379581356720654'
        # varmail, mix, f2fs
        # 'wear-all-apps-by-celery3/subexp--539875531819358192-f2fs-10-12-22-13-00--5922920594200190908'
        # leveldb, seq, xfs
        # 'wear-all-apps-by-celery3/subexp--306904456059016887-xfs-10-12-21-59-21-5722651436764932430'
        # varmail, ext4, large file set
        # 'varmail-large-ext4'
        'sqliterb-wear-20m-makeup4/subexp-6472841463288497858-f2fs-10-16-23-12-05-4456356332077770427'

                   ),
                 # c('top')
                 c('bottom')
                 )




}
main()

