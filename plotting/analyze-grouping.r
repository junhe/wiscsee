# libraries
library(ggplot2)
library(plyr)
library(dplyr)
library(reshape2)
library(gridExtra)
library(jsonlite)
library(digest)
library(splitstackshape)
library(gganimate)

source('doraemon/header.r')
source('doraemon/organizers.r')
source('doraemon/file_parsers.r')

# copy the following so you can do sme()
# setwd(WORKDIRECTORY)
sme <- function()
{
    WORKDIRECTORY= "/Users/junhe/workdir/analysis-script/"
    THISFILE     ='doraemon/analyze-grouping.r'
    setwd(WORKDIRECTORY)
    source(THISFILE)
}



# -------------- class diagram ----------------
# SubExpGrouping -- TrafficPlot
# SubExpStableRatios -- CometGraph



aggregate_subexp_list_to_dataframe <- function(exp_rel_path, sub_analyzer)
{
    result_list = list()
    for (path in exp_rel_path) {
        subexpiter = SubExpIter(exp_rel_path=exp_rel_path)
        result = subexpiter$iter_each_subexp(sub_analyzer)
        result_list = append(result_list, result)
    }
    result = lapply(result, unlist)
    d = do.call('rbind', result)
    d = data.frame(d)
    return(d)
}

sample_down <- function(d, interval_ratio)
{
    n = nrow(d)
    sel = seq(1, n, max(as.integer(n * interval_ratio), 1))
    return(d[sel,  ])
}

sample_count <- function(d, count)
{
    n = nrow(d)
    sel = seq(1, n, length.out=count)
    return(d[sel,  ])
}


set_snake_points_for_a_snapshot <- function(d) {
    d = arrange(d, desc(valid_ratio))
    d = transform(d, seg_end = cumsum(count))
    d = transform(d, seg_start = seg_end - count)
    d = melt(d, 
         # id=c('valid_ratio', 'segment_bytes', 'blocksize', 'filesystem'), 
         measure = c('seg_start', 'seg_end'), value.name = 'blocknum')
    d = arrange(d, desc(valid_ratio))

}

set_moving_snake_points_for_a_snapshot <- function(d) {
    d = arrange(d, valid_ratio)
    d = transform(d, seg_end = cumsum(count))
    d = transform(d, seg_start = seg_end - count)
    d = melt(d, 
         # id=c('valid_ratio', 'segment_bytes', 'blocksize', 'filesystem'), 
         measure = c('seg_start', 'seg_end'), value.name = 'blocknum')
    d = arrange(d, valid_ratio)
}


c_______sub_analyser____________ <- function(){}



SubExpGrouping <- setRefClass("SubExpGrouping",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            confjson = create_configjson(subexppath=subexppath)
            recjson = RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            pagesize=as.numeric(confjson$get_page_size())
            blocksize=as.numeric(confjson$get_block_size())
            
            l = list(
                blocksize=confjson$get_block_size(),
                pagesize=confjson$get_page_size(),
                filesystem=confjson$get_exp_parameters_filesystem(),
                segment_bytes=confjson$get_segment_bytes(),
                discarded_bytes=recjson$get_discard_traffic_size(),
                read_bytes=recjson$get_read_traffic_size(),
                write_bytes=recjson$get_write_traffic_size(),
                workload_class=confjson$get_workload_class(),
                flash_w_bytes=recjson$get_flash_ops_write() * pagesize,
                flash_r_bytes=recjson$get_flash_ops_read() * pagesize,
                flash_e_bytes=recjson$get_flash_ops_erase() * blocksize
                )
            return(l)
        }
    )
)

TrafficPlot <- setRefClass("TrafficPlot",
    methods = list(
        plot = function(exp_rel_path, testclass)
        {
            d = aggregate_subexp_list_to_dataframe(exp_rel_path, SubExpGrouping)
            d = remove_dup(d, c('workload_class', 'filesystem'))

            d = melt(d, id=c('workload_class', 'filesystem', 
                             'segment_bytes', 'blocksize'), 
                     measure=c('write_bytes', 'read_bytes', 'discarded_bytes'))
            d = plyr::rename(d, c('variable'='operation',
                            'value'='size'))
            d$operation = revalue(d$operation, c('write_bytes'='write', 
                                               'read_bytes'='read',
                                               'discarded_bytes'='discard'))
            d = subset(d, operation %in% c('write', 'discard'))

            d = transform(d, size = as.numeric(size)/MB)

            p = ggplot(d, aes(x=filesystem, y=size, fill=operation)) +
                geom_bar(aes(order=desc(operation)), stat='identity', position='dodge') +
                facet_grid(~workload_class) +
                scale_y_continuous(breaks=seq(0, 1024*10, 1024),
                                   labels=seq(0, 1024*10, 1024)/1024) +
                scale_fill_grey_dark() +
                xlab('file system') +
                ylab('size (GB)') + 
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                theme(panel.margin.x = unit(1, "lines")) +
                theme(legend.position='top', legend.title=element_blank())
            print(p)

        }
    )
)


c_______Stable_Ratio_Curve_______________ <- function(){}

SubExpStableRatios <- setRefClass("SubExpStableRatios",
    fields = list(subexppath="character",
                  confjson="ConfigJson",
                  recjson="RecorderJson"
                  ),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            recjson <<- RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            d = get_lastest_valid_ratios_()
            d = transform(d, filesystem=confjson$get_exp_parameters_filesystem())
            d = transform(d, workload_class=confjson$get_workload_class())
            d = transform(d, blocksize=confjson$get_block_size())
            d = transform(d, segment_bytes=confjson$get_segment_bytes())
            d = transform(d, expname=confjson$get_expname())
            d = transform(d, appnames=confjson$get_appmix_appnames())
            d = transform(d, appname=confjson$get_appmix_first_appname())

            return(d)
        }, 

        get_lastest_valid_ratios_ = function()
        {
            if (length(recjson$get_valid_ratios()) > 0) {
                print('old style')
                d = get_lastest_valid_ratios_old_()
            } else {
                print('new style')
                d = get_lastest_valid_ratios_new_()
            }
            return(d)
        },
        get_lastest_valid_ratios_old_ = function()
        {
            ratios = recjson$get_valid_ratios()
            d = data.frame(count=unlist(ratios))
            d$valid_ratio = as.numeric(rownames(d))
            return(d)
        },
        get_lastest_valid_ratios_new_ = function()
        {
            snapshots = recjson$get_valid_ratio_snapshots()
            dd = tail(snapshots, 1)
            dd = melt(as.matrix(dd))
            names(dd) = c('snapshot_id', 'valid_ratio', 'count')
            dd = subset(dd, is.na(count) == FALSE)
            dd$snapshot_id = NULL
            return(dd)
        }
    )
)

# DON't Modify this one, this is kept for paper plot
StableStateLineGraph <- setRefClass("StableStateLineGraph",
    fields = list(exp_rel_path='character', d_='data.frame'),
    methods = list(
        initialize = function(rel_path)
        {
            exp_rel_path <<- rel_path
            d_ <<- aggregate_subexp_dataframes(exp_rel_path, SubExpStableRatios)
        },
        plot_line_graph = function()
        {
            d = d_

            # filter out bad result
            d = subset(d, !(expname == 'varmail-add-btrfs-xfs-go' &
                            segment_bytes == 2*GB &
                            blocksize == 1*MB &
                            workload_class == 'Varmail' &
                            filesystem == 'btrfs'
                            ))

            d = fast_organize_(d)
            d = transform(d, ftlconf = interaction(to_human_factor(segment_bytes),
                                                   to_human_factor(blocksize)))

            d = transform(d, block_location = (as.numeric(blocknum)/GB) * as.numeric(blocksize))
            d = subset(d, valid_ratio != 0)

            d = subset(d, segment_bytes != 2*MB)
            # d = subset(d, blocksize == 1*MB & segment_bytes == 2*GB)
            d = subset(d, blocksize == 1*MB)
            d = subset(d, filesystem != 'btrfs')

            plot_all_apps_in_one_(d)
        },

        plot_all_apps_in_one_ = function(d)
        {

            d = transform(d, ftlconf = to_human_factor(segment_bytes))
            d = decor_factor_levels_2(d, 'SEG:', 'ftlconf', '')
            d = transform(d, filesystem = factor(filesystem, 
                     levels=c('btrfs', 'ext4', 'f2fs', 'xfs')))

            p = ggplot(d) +
                geom_line(aes(x=block_location, y=valid_ratio, color=filesystem))+
                facet_grid(ftlconf~workload_class, space='free_x', scale='free_x') +
                # geom_vline(xintercept=fs_size_gb, linetype=2, color='grey') +
                ylab('Valid Ratio') +
                xlab('Cumulative Block Size (GB)') +
                scale_color_manual(
                   values=get_color_map2(c('btrfs', 'ext4', 'f2fs', 'xfs'))) +
                scale_x_continuous(breaks=seq(0, 10)) +
                theme_zplot() +
                theme(panel.margin = unit(0.3, "lines"))
            save_plot(p, 'grouping-snake-graph-single-app', save=T, w=3.2, h=2.5)
            print(p)
        },
        fast_organize_ = function(d)
        {
            d = ddply(d, .(segment_bytes, 
                           filesystem, 
                           workload_class, 
                           blocksize), 
                      set_snake_points_for_a_snapshot)
            return(d)
        }
    )
)

AppMixSnakeGraph <- setRefClass("ExploreStableStateLineGraph",
    fields = list(exp_rel_path='character', d_='data.frame'),
    methods = list(
        initialize = function(rel_path)
        {
            exp_rel_path <<- rel_path
            d_ <<- aggregate_subexp_dataframes(exp_rel_path, SubExpStableRatios)
        },
        plot_line_graph = function()
        {
            d = d_

            d = fast_organize_(d)
            d = transform(d, block_location = (as.numeric(blocknum)/GB) * as.numeric(blocksize))
            d = subset(d, valid_ratio != 0)
            d = subset(d, blocksize == 1*MB)

            plot_all_apps_in_one_(d)
        },

        plot_all_apps_in_one_ = function(d)
        {
            d = transform(d, ftlconf = to_human_factor(segment_bytes))
            # d = decor_factor_levels_2(d, 'SEG:', 'ftlconf', '')
            print(levels(d$ftlconf))
            levels(d$ftlconf) = plyr::revalue(levels(d$ftlconf),
                    c("128 MB"='Segmented', "2 GB"='Non-Seg')) 

            d = subset(d, filesystem != 'btrfs')
            d = transform(d, filesystem = factor(filesystem, 
                 levels=c('btrfs', 'ext4', 'f2fs', 'xfs')))
            p = ggplot(d) +
                geom_line(aes(x=block_location, y=valid_ratio, 
                              color=filesystem), size=0.4)+
                facet_grid(ftlconf~appname, space='free_x', scale='free_x') +
                # geom_vline(xintercept=fs_size_gb, linetype=2, color='grey') +
                ylab('Valid Ratio') +
                xlab('Cumulative Block Space') +
                scale_color_manual(
                   values=get_fs_color_map(c('btrfs', 'ext4', 'f2fs', 'xfs'))) +
                # scale_x_continuous(limits=c(0, 1)) +
                # scale_color_grey_dark() +
                scale_x_continuous(breaks=seq(0, 10, 0.5)) +
                theme_zplot() +
                theme(panel.margin = unit(0.3, "lines"))
            # save_plot(p, 'grouping-snake-graph-mix', save=T, w=6, h=3)
            save_plot(p, 'grouping-snake-graph-mix', save=T, w=3.3, h=3)
            print(p)
        },


        fast_organize_ = function(d)
        {
            d = ddply(d, .(segment_bytes, 
                           filesystem, 
                           workload_class, 
                           appname,
                           expname,
                           blocksize), 
                      set_snake_points_for_a_snapshot)
            return(d)
        }
    )
)

tmpAppMixSnakeGraph <- setRefClass("tmpExploreStableStateLineGraph",
    fields = list(exp_rel_path='character', d_='data.frame'),
    methods = list(
        initialize = function(rel_path)
        {
            exp_rel_path <<- rel_path
            d_ <<- aggregate_subexp_dataframes(exp_rel_path, SubExpStableRatios)
        },
        plot_line_graph = function()
        {
            d = d_

            d = fast_organize_(d)
            d = transform(d, block_location = (as.numeric(blocknum)/GB) * as.numeric(blocksize))
            d = subset(d, valid_ratio != 0)
            d = subset(d, blocksize == 1*MB)

            plot_all_apps_in_one_(d)
        },

        plot_all_apps_in_one_ = function(d)
        {
            d = transform(d, ftlconf = to_human_factor(segment_bytes))
            d = decor_factor_levels_2(d, 'SEG:', 'ftlconf', '')

            d = subset(d, filesystem != 'btrfs')
            d = transform(d, filesystem = factor(filesystem, 
                 levels=c('btrfs', 'ext4', 'f2fs', 'xfs')))
            d = transform(d, filesystem = interaction(filesystem, expname))
            p = ggplot(d) +
                geom_line(aes(x=block_location, y=valid_ratio, 
                              color=filesystem))+
                facet_grid(ftlconf~appnames, space='free_x', scale='free_x') +
                # geom_vline(xintercept=fs_size_gb, linetype=2, color='grey') +
                ylab('Valid Ratio') +
                xlab('Cumulative Block Size (GB)') +
                # scale_x_continuous(limits=c(0, 1)) +
                # scale_color_grey_dark() +
                scale_x_continuous(breaks=seq(0, 10, 0.5)) +
                theme_zplot() +
                theme(panel.margin = unit(0.3, "lines"))
            save_plot(p, 'grouping-snake-graph-mix', save=T, w=6, h=3)
            print(p)
        },


        fast_organize_ = function(d)
        {
            d = ddply(d, .(segment_bytes, 
                           filesystem, 
                           workload_class, 
                           appnames,
                           expname,
                           blocksize), 
                      set_snake_points_for_a_snapshot)
            return(d)
        }
    )
)



c_______Working_Set____________________ <- function(){}

SubExpWorkingSet <- setRefClass("SubExpWorkingSet",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            confjson = create_configjson(subexppath=subexppath)
            statsjson = StatsJson(filepath=file.path(subexppath, 'stats.json'))

            ret = list(
                written_bytes=statsjson$get_write_traffic_size(),
                valid_data_bytes=statsjson$get_disk_used_bytes(),
                num=confjson$get_leveldb_benchconf_num(),
                max_key=confjson$get_leveldb_max_key(),
                workload_class=confjson$get_workload_class()
                )

            return(ret)
        }
    )
)
 
 
WorkingSetPlot <- setRefClass("WorkingSetPlot",
    fields = list(expname='character'),
    methods = list(
        run = function(exp_rel_path, testclass)
        {
            d = aggregate_subexp_list_to_dataframe(exp_rel_path, 
                                               SubExpWorkingSet)
            d = cols_to_num(d, c('written_bytes',
                                 'valid_data_bytes'))

            plot_disk_used_size(d)
        }, 
        plot_disk_used_size = function(d)
        {
            d = transform(d, written_bytes=written_bytes / MB)
            d = transform(d, valid_data_bytes=valid_data_bytes / MB)

            d = remove_dup(d, by_col='workload_class')

            p = ggplot(d, aes(x=workload_class, y=valid_data_bytes)) +
                geom_bar(stat='identity', position='dodge') +
                theme_zplot() +
                ylab("App Data Size (MB)") +
                xlab("")
            print(p)
        }
    )
)


c_______GC_LOG____________________________ <- function(){}

SubExpGcLogParsed <- setRefClass("SubExpGcLogParsed",
    fields = list(subexppath="character", pagesize="numeric", expname='character'),
    methods = list(
        run = function()
        {
            confjson = create_configjson(subexppath=subexppath)
            pagesize <<- confjson$get_page_size()
            expname <<- confjson$get_expname()
            recjson = RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            gclog = GcLogParsed(
                  filepath=paste(subexppath, 'gc.log.parsed', sep='/'))

            d = gclog$load()
            d = transform(d, semantics=trim(semantics),
                             valid=as.logical(trim(valid)))
            d = transform(d, data_type=semantics)


            plot_data_moved(d)
            # plot_types_of_data_mixed(d)
            # plot_type_mixes_count(d)
            # plot_combo_valid_and_type(d)
            # plot_max_lpn_dist_of_unknown(d, pagesize=confjson$get_page_size())
            # plot_lpns(d, pagesize=confjson$get_page_size())
        }, 
        plot_data_moved = function(d)
        {
            d = as.data.frame(table(data_type=d$semantics, page_valid=d$valid))
            print(d)
            d = subset(d, page_valid==TRUE)
            d = transform(d, moved_size=Freq*pagesize/MB)
            print(d)
            p = ggplot(d, aes(x=data_type, y=moved_size)) +
                geom_bar(stat='identity') +
                ggplot_common_addon() +
                ggtitle(expname)
            print(p)
        },
        plot_types_of_data_mixed = function(d)
        {
            d = as.data.frame(table(data_type=d$semantics, gcid=d$gcid))
            d = subset(d, Freq > 0)
            d = as.data.frame(table(gcid=d$gcid))
            d = plyr::rename(d, c('Freq'='n_types_per_block'))
            
            p = ggplot(d, aes(x=n_types_per_block)) +
                geom_histogram() +
                ggplot_common_addon() +
                ggtitle(expname)
            print(p)
        },
        plot_type_mixes_count = function(d)
        {
            form_mix_str <- function(dd)
            {
                mix = paste(sort(unique(as.character(dd$data_type))),
                            collapse='+')
                return(c('mix'=mix))
            }
            # count of each mix, for example, there are 300 blocks
            # that has 'superblock+journal'
            d = ddply(d, .(gcid), form_mix_str)
            print(head(d))

            d = as.data.frame(table(mix=d$mix))

            p = ggplot(d, aes(x=mix, y=Freq)) +
                geom_bar(stat='identity') +
                geom_text(aes(label=Freq)) +
                ggplot_common_addon() +
                ggtitle(expname)
            print(p)
        },
        plot_what_type_is_invalidated = function(d)
        {
            # with mix 'journal+UNKNOWN'
            find_invalidated <- function(d)
            {
                all_types = paste(sort(unique(as.character(d$data_type))), collapse='+')
                if (all_types != 'journal+UNKNOWN') 
                    return(NULL)

                invalid_types = paste(
                  sort(unique(subset(d, valid==FALSE)$data_type)), collapse='+')
                return(c('inv_types'=invalid_types))
            }

            d = transform(d, data_type=semantics)

            d = ddply(d, .(gcid), find_invalidated)
            if (nrow(d) == 0)
                return()

            d = as.data.frame(table(inv_types=d$inv_types))

            p = ggplot(d, aes(x=inv_types, y=Freq)) +
                geom_bar(stat='identity') +
                ggplot_common_addon() +
                ggtitle(paste(expname, 'Among block with journal+UNKNOWN'))
            print(p)
        },
        plot_combo_valid_and_type = function(d)
        {
            find_combo <- function(d)
            {
                all_types = paste(sort(unique(as.character(d$data_type))), collapse='+')
                if (all_types != 'journal+UNKNOWN') 
                    return(NULL)

                valid_types = paste(
                  sort(unique(subset(d, valid==TRUE)$data_type)), collapse='+')
                invalid_types = paste(
                  sort(unique(subset(d, valid==FALSE)$data_type)), collapse='+')
                return(c('combo'=paste(
                        'V:', valid_types, '  |  ',
                        'INV:', invalid_types,
                        sep=''),
                         'n_val_pages'=nrow(subset(d, valid==TRUE))
                         ))
            }
            d = ddply(d, .(gcid), find_combo)
            if (nrow(d) == 0)
                return()

            d = cols_to_num(d, c('n_val_pages'))

            d.cnt = as.data.frame(table(combo=d$combo))

            p = ggplot(d.cnt, aes(x=combo, y=Freq)) +
                geom_bar(stat='identity') +
                ggplot_common_addon() +
                ggtitle('Among block with journal+UNKNOWN')
            print(p)

            d.stats = aggregate(n_val_pages~combo, data=d, sum)
            d.stats = transform(d.stats, moved_size=n_val_pages*pagesize/MB)
            p = ggplot(d.stats, aes(x=combo, y=moved_size)) +
                geom_bar(stat='identity') +
                ggplot_common_addon() +
                ggtitle(paste(expname, 'Among block with journal+UNKNOWN'))
            print(p)
        },
        plot_max_lpn_dist_of_unknown = function(d, pagesize)
        {
            find_distance <- function(d)
            {
                all_types = paste(sort(unique(as.character(d$data_type))), collapse='+')
                if (all_types != 'UNKNOWN') 
                    return(NULL)
                max_dist = max(d$lpn) - min(d$lpn)
                return(c('max_dist'=max_dist))
            }
            d = ddply(d, .(gcid), find_distance)
            d = transform(d, max_dist = max_dist*pagesize/MB)

            p = ggplot(d, aes(x=seq_along(max_dist), y=max_dist)) +
                geom_point() +
                ggtitle(expname)
            print(p)
        },
        plot_lpns = function(d, pagesize)
        {
            max_gcid = max(d$gcid)
            samplegcids = sample(seq(max_gcid), 50)
            d = subset(d, gcid %in% samplegcids)
            d = transform(d, offset = lpn*pagesize/MB)
            p = ggplot(d, aes(x=factor(gcid), 
                              y=offset,
                              color=valid
                              )) +
                geom_point() +
                ggplot_common_addon() +
                ggtitle(expname)
                # scale_y_continuous(breaks=seq(0, 1024, 16), limits=c(0, 1024)) 
            print(p)
        }
    )
)

GcLogPlot <- setRefClass("GcLogPlot",
    fields = list(),
    methods = list(
        run = function(exp_rel_path)
        {
            subexpiter = SubExpIter(exp_rel_path=exp_rel_path)
            result = subexpiter$iter_each_subexp(SubExpGcLogParsed)
        }
    )
)




c_______comet_movie_______________________ <- function(){}

SubExpCometMovieAggregator <- setRefClass("SubExpCometMovieAggregator",
    fields = list(expname='character'),
    methods = list(
        run = function(exp_rel_path, testclass)
        {
            d = aggregate_subexp_list_to_dataframe(exp_rel_path, 
                                               SubExpCometMovie)
        }
    )
)

SubExpCometMovie <- setRefClass("SubExpCometMovie",
    fields = list(subexppath="character", confjson='ConfigJson', recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            recjson <<- RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            snapshots = recjson$get_valid_ratio_snapshots()

            d = organize_snapshots(snapshots)
            d = transform(d, filesystem=confjson$get_filesystem())
            d = transform(d, workload_class=confjson$get_workload_class())
            d = transform(d, blocksize=confjson$get_block_size())
            d = transform(d, segment_bytes=confjson$get_segment_bytes())

            name = paste(
                confjson$get_workload_class(),
                confjson$get_filesystem(),
                to_human_unit(confjson$get_segment_bytes()), 
                to_human_unit(confjson$get_block_size()),
                sep='-')
            print(name)

            plot_curves_of_snapshots_fast(d)
            # plot_movie_fast(d, name=name)
            # animate_moving_snake(d, name)

            return(list())
        },

        animate_moving_snake = function(d, name)
        {
            d = ddply(d, .(snapshot_id), set_moving_snake_points_for_a_snapshot)
            workload_class = confjson$get_workload_class()
            fs_size_gb = confjson$get_lbabytes() / GB

            d = transform(d, ftlconf = interaction(to_human_factor(segment_bytes),
                                                   to_human_factor(blocksize)))

            d = transform(d, block_location = (as.numeric(blocknum)/GB) * as.numeric(blocksize))

            # d = sample_snapshots(d, 4)
            d = sample_snapshots(d, 100)

            p = ggplot(d, aes(frame=snapshot_id)) +
                geom_ribbon(aes(x=block_location, ymin=0, ymax=1), fill='grey') +
                geom_ribbon(aes(x=block_location, ymin=0, ymax=valid_ratio), fill='black') +
                facet_grid(ftlconf~filesystem, space='free_x', scale='free_x') +
                geom_vline(xintercept=fs_size_gb, linetype=2, color='grey') +
                ylab('Valid Ratio') +
                xlab('Accumulated Block Size (GB)') +
                scale_zplot() +
                theme_zplot() +
                theme(panel.margin = unit(2, "lines")) +
                theme(legend.position="none") +
                ggtitle(workload_class)
            # print_plot(p)
            gg_animate(p, paste('moving-snake', name, 'gif', sep='.'), interval=0.1)
        },


        plot_curves_of_snapshots_fast = function(d)
        {
            d = fast_organize(d)
            fs_size_gb = confjson$get_lbabytes() / GB

            d = transform(d, ftlconf = interaction(to_human_factor(segment_bytes),
                                                   to_human_factor(blocksize)))

            d = transform(d, block_location = (as.numeric(blocknum)/GB) * as.numeric(blocksize))

            d = sample_snapshots(d, 100)

            p = ggplot(d) +
                geom_line(aes(x=block_location, y=valid_ratio, 
                              color=factor(snapshot_id))) +
                facet_grid(ftlconf~filesystem, space='free_x', scale='free_x') +
                geom_vline(xintercept=fs_size_gb, linetype=2, color='grey') +
                ylab('Valid Ratio') +
                xlab('Accumulated Block Size (GB)') +
                scale_zplot() +
                theme_zplot() +
                # scale_x_continuous(limits=c(0, 2))
                theme(panel.margin = unit(2, "lines")) +
                theme(legend.position="none")
            # print(p)
            save_plot(p, paste('curve-snapshots', Sys.time()) , save=T, w=4, h=4, ext='png') 
        },

        plot_movie_fast = function(d, name='noname')
        {
            d = fast_organize(d)
            fs_size_gb = confjson$get_lbabytes() / GB

            d = transform(d, ftlconf = interaction(to_human_factor(segment_bytes),
                                                   to_human_factor(blocksize)))
            d = transform(d, block_location = (as.numeric(blocknum)/GB) * as.numeric(blocksize))

            d = sample_snapshots(d, 100)

            p = ggplot(d, aes(frame=snapshot_id)) +
                geom_ribbon(aes(x=block_location, ymin=0,  ymax=valid_ratio)) +
                geom_vline(xintercept=fs_size_gb, linetype=2, color='grey') +
                facet_grid(ftlconf~filesystem, space='free_x', scale='free_x') +
                ylab('Valid Ratio') +
                xlab('Accumulated Block Size (GB)') +
                scale_zplot() +
                theme_zplot() +
                theme(panel.margin = unit(2, "lines"))
            # print(gg_animate(p, interval=0.1))
            gg_animate(p, paste(name, 'gif', sep='.'), interval=0.1)
        },

        plot_snapshots_rebbon = function(d)
        {
            d = organize_data(d)
            d = arrange(d, valid_ratio)

            p = ggplot(d) +
                geom_ribbon(aes(x=block_location, ymin=0,  ymax=valid_ratio)) +
                geom_vline(xintercept=1, size=1, linetype=2, color='#bdbdbd') +
                facet_grid(snapshot_id~workload_class, space='free_x', scale='free_x') +
                ylab('Valid Ratio') +
                xlab('Accumulated Block Size (GB)') +
                scale_zplot() +
                theme_zplot() +
                theme(panel.margin = unit(2, "lines"))
            print_plot(p)
        },

        organize_snapshots = function(snapshots){
            snapshot_interval = confjson$get_snapshot_interval() 
            gc_start_time = recjson$get_gc_start_timestamp()

            d = melt(as.matrix(snapshots))
            d = as.data.frame(d)
            names(d) = c('snapshot_id', 'valid_ratio', 'count')
            d = subset(d, is.na(count) == FALSE)

            d = transform(d, snapshot_time = snapshot_id * snapshot_interval)
            d = subset(d, snapshot_time < gc_start_time)

            return(d)
        },
        fast_organize = function(d)
        {
            d = ddply(d, .(snapshot_id), set_snake_points_for_a_snapshot)
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




c_______benchmark_classes__________ <- function(){}


TrafficSizePlot <- setRefClass("TrafficSizePlot",
    fields = list(expname='character'),
    methods = list(
        run = function(exp_rel_path, testclass)
        {
            d = aggregate_subexp_list_to_dataframe(exp_rel_path, 
                                               SubExpGrouping)
            d = cols_to_num(d, c(
                                 'pagesize',
                                 'segment_bytes',
                                 'flash_w_bytes',
                                 'flash_r_bytes',
                                 'flash_e_bytes',
                                 'write_bytes',
                                 'read_bytes',
                                 'discarded_bytes'
                                 ))





            plot_traffic_paper(d)
        },
        plot_traffic_paper = function(d)
        {
            d = melt(d, id=c('filesystem', 'segment_bytes', 'benchname'), 
                     measure=c('write_bytes', 'read_bytes', 'discarded_bytes'))
            d = plyr::rename(d, c('variable'='operation', 'value'='size'))
            d$operation = revalue(d$operation, c('write_bytes'='write', 
                                               'read_bytes'='read',
                                               'discarded_bytes'='discard'))
            d = subset(d, operation %in% c('write', 'discard'))

            d = transform(d, size = as.numeric(size)/MB)
            
            d = transform(d, benchname=revalue(benchname,
                c('Sqlite-random'='Sqlite-rand', 'Sqlite-sequential'='Sqlite-seq')))

            d = cols_to_num(d, c('segment_bytes'))
            d = subset(d, segment_bytes==2*MB)
            d = remove_dup(d, by_col=c('filesystem', 'benchname', 'operation'))
            print(d)

            p = ggplot(d, aes(x=filesystem, y=size, fill=operation)) +
                geom_bar(aes(order=desc(operation)), stat='identity', position='dodge') +
                facet_grid(segment_bytes~benchname) +
                # scale_y_continuous(breaks=seq(0, 1024*10, 1024),
                                   # labels=seq(0, 1024*10, 1024)/1024) +
                # ylab('size (GB)') + 
                scale_fill_grey_dark() +
                xlab('file system') +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                theme(panel.margin.x = unit(1, "lines")) +
                theme(legend.position='top', legend.title=element_blank())
            print(p)
            save_plot(p, paste('grouping-app-traffic', sep=''), save=T, h=3.2, w=5)
        }
    )
)


CometGraph <- setRefClass("CometGraph",
    fields = list(expname='character'),
    methods = list(
        run = function(exp_rel_path, testclass)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpStableRatios)

            d = cols_to_num(d, c('valid_ratio', 'blocksize'))

            plot_comet_graph_x_is_byte_line_compare_fs(d)
        },
        plot_comet_graph_x_is_byte_line_compare_fs = function(d)
        {
            d = organize_data(d)

            d = transform(d, segment_bytes=to_human_factor(segment_bytes))
            d = transform(d, ftlconf = interaction(segment_bytes, to_human_factor(blocksize)))
            d = subset(d, valid_ratio > 0)
            print(head(d))
            d = transform(d, block_location=(blocksize/GB) * blockid)
            p = ggplot(d) +
                geom_line(aes(x=block_location, y=valid_ratio, color=filesystem),
                          size=1) +
                geom_vline(xintercept=1, size=1, linetype=2, color='#bdbdbd') +
                facet_grid(ftlconf~workload_class, space='free_x', scale='free_x') +
                ylab('Valid Ratio') +
                xlab('Accumulated Block Size (GB)') +
                scale_zplot() +
                theme_zplot() +
                theme(panel.margin = unit(2, "lines"))
            print(p)
            save_plot(p, 'grouping-comet-graph', save=T, w=10, h=12)
        },
        organize_data = function(d)
        {
            d = expandRows(d, "count")
            d = arrange(d, desc(valid_ratio))
            d = ddply(d, .(segment_bytes, blocksize, filesystem, workload_class), 
                      transform, blockid=seq_along(valid_ratio))

            return(d)
        }

    )
)


c_______Side_by_Side_________________ <- function(){}

SubExpSnakeCurve <- setRefClass("SubExpSnakeCurve",
    fields = list(subexppath="character", confjson='ConfigJson', recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            recjson <<- RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            snapshots = recjson$get_valid_ratio_snapshots()

            d = organize_snapshots(snapshots)
            d = transform(d, filesystem=confjson$get_filesystem())
            d = transform(d, workload_class=confjson$get_workload_class())
            d = transform(d, blocksize=confjson$get_block_size())
            d = transform(d, segment_bytes=confjson$get_segment_bytes())

            name = paste(
                confjson$get_workload_class(),
                confjson$get_filesystem(),
                to_human_unit(confjson$get_segment_bytes()), 
                to_human_unit(confjson$get_block_size()),
                sep='-')

            return(d)
        },
        organize_snapshots = function(snapshots){
            snapshot_interval = confjson$get_snapshot_interval() 
            gc_start_time = recjson$get_gc_start_timestamp()

            d = melt(as.matrix(snapshots))
            d = as.data.frame(d)
            names(d) = c('snapshot_id', 'valid_ratio', 'count')
            d = subset(d, is.na(count) == FALSE)

            d = transform(d, snapshot_time = snapshot_id * snapshot_interval)
            d = subset(d, snapshot_time < gc_start_time)

            return(d)
        },

        end = function(){}
    )
)





SideBySideCurvePlot <- setRefClass("SideBySideCurvePlot",
    fields = list(exp_rel_path='character', d_='data.frame'),
    methods = list(
        initialize = function(path)
        {
            exp_rel_path <<- path
            d_ <<- aggregate_subexp_dataframes(exp_rel_path, SubExpSnakeCurve)
            stopifnot(length(unique(d_$workload_class)) == 1)
        },
        plot_side_by_side = function()
        {
            plot_curves_of_snapshots_fast_(d_)
        },


        plot_curves_of_snapshots_fast_ = function(d)
        {
            print(head(d))
            d = fast_organize_(d)

            d = transform(d, ftlconf = interaction(to_human_factor(segment_bytes),
                                                   to_human_factor(blocksize)))

            d = transform(d, block_location = (as.numeric(blocknum)/GB) * as.numeric(blocksize))

            d = sample_snapshots_(d, 100)

            p = ggplot(d) +
                geom_line(aes(x=block_location, y=valid_ratio, 
                              color=factor(snapshot_id))) +
                facet_grid(ftlconf~filesystem, space='free_x', scale='free_x') +
                # geom_vline(xintercept=fs_size_gb, linetype=2, color='grey') +
                ylab('Valid Ratio') +
                xlab('Accumulated Block Size (GB)') +
                scale_zplot() +
                theme_zplot() +
                theme(panel.margin = unit(2, "lines")) +
                theme(legend.position="none")
            print(p)
            save_plot(p, 'curve-snapshots', save=T, w=3.2, h=2.8) 
        },
        fast_organize_ = function(d)
        {
            d = ddply(d, .(snapshot_id, segment_bytes, filesystem, workload_class, blocksize), 
                      set_snake_points_for_a_snapshot)
            return(d)
        },
        sample_snapshots_ = function(d, n)
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


c_______NEW_RATIOS__________ <- function(){}

SubExpStableRatiosNEW <- setRefClass("SubExpStableRatiosNEW",
    fields = list(subexppath="character",
                  confjson="ConfigJson",
                  recjson="RecorderJson"
                  ),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            recjson <<- RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            d = get_lastest_valid_ratios_()
            d = transform(d, filesystem=confjson$get_exp_parameters_filesystem())
            d = transform(d, workload_class=confjson$get_workload_class())
            d = transform(d, blocksize=confjson$get_block_size())
            d = transform(d, segment_bytes=confjson$get_segment_bytes())
            d = transform(d, expname=confjson$get_expname())
            d = transform(d, appnames=confjson$get_appmix_appnames())
            d = transform(d, 
                          appname=confjson$get_testname_appname(),
                          testname=confjson$get_testname(),
                          rw=confjson$get_testname_rw(),
                          pattern=confjson$get_testname_pattern())
 

            return(d)
        }, 

        get_lastest_valid_ratios_ = function()
        {
            d = get_lastest_valid_ratios_new_()
            return(d)
        },
        get_lastest_valid_ratios_new_ = function()
        {
            snapshots = recjson$get_valid_ratio_snapshots()
            dd = tail(snapshots, 1)
            dd = melt(as.matrix(dd))
            names(dd) = c('snapshot_id', 'valid_ratio', 'count')
            dd = subset(dd, is.na(count) == FALSE)
            dd$snapshot_id = NULL
            return(dd)
        }
    )
)

ValidRatioGraph <- setRefClass("ValidRatioGraph",
    fields = list(),
    methods = list(
       plot_line_graph = function(exp_rel_path)
        {
            d = get_data(exp_rel_path)

            plot_all_apps_in_one_(d)
        },

        plot_all_apps_in_one_ = function(d)
        {
            d = rename_pattern_levels(d)
            d = shorten_appnames(d)

            d = transform(d, segmentation = to_human_factor(segment_bytes))
            levels(d$segmentation) = plyr::revalue(levels(d$segmentation),
                    c("128 MB"='S', "2 GB"='N')) 
            d = get_hfacets(d)
            d$vfacets = interaction(d$pattern)

            d = transform(d, filesystem = factor(filesystem, 
                                 levels=c('ext4', 'f2fs', 'xfs')))
            p = ggplot(d) +
                geom_line(aes(x=block_location, y=valid_ratio, 
                              color=filesystem, 
                              # linetype=filesystem, 
                              size=filesystem))+
                geom_vline(xintercept=1, size=0.3, color='gray', linetype='dashed') +
                facet_grid(vfacets~hfacets, space='free_x', scale='free_x') +
                ylab('Valid Ratio') +
                xlab('Cumulative Block Space') +
                scale_color_manual(values=get_fs_color_map(NULL)) +
                scale_linetype_manual(values=get_fs_linetype_map()) +
                scale_size_manual(values=get_fs_size_map()) +
                scale_y_continuous(breaks=seq(0, 1)) +
                scale_x_continuous(breaks=seq(0, 10, 1)) +
                theme_zplot() +
                theme(panel.margin.y = unit(0.4, "lines")) +
                theme(panel.margin.x = unit(0.4, "lines")) +
                theme(strip.text.x = element_text(size=7)) +
                theme(strip.text.y = element_text(size=5.3)) +
                theme(legend.background = element_blank()) +
                theme(legend.position=c(0.45, 0.9), 
                      legend.direction="vertical",
                      legend.title=element_blank())

                # theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            # save_plot(p, 'grouping-snake-graph-mix', save=T, w=6, h=3)
            save_plot(p, 'grouping-snake-graph-mix', save=T, w=7, h=1.4)
            print(p)
        },
        get_hfacets = function(d)
        {
            d$segmentation = revalue(d$segmentation, c('N'='n', 'S'='s'))
            d$hfacets = interaction(d$appname, d$segmentation, sep = '\n')
            d$hfacets = factor(d$hfacets, levels = sort(levels(d$hfacets)))
            # l1 = levels(d$appname)
            # l2 = levels(d$segmentation)

            # n1 = length(l1)
            # n2 = length(l2)

            # l1 = rep(l1, each=n2)
            # l2 = rep(l2, n1)
            # new_levels = paste(l1, l2, sep='.')

            # d$hfacets = factor(d$hfacets, levels=new_levels)
            return(d)
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
            d <- aggregate_subexp_dataframes(exp_rel_path, SubExpStableRatiosNEW)
            d = fast_organize_(d)
            d = transform(d, block_location = (as.numeric(blocknum)/GB) * as.numeric(blocksize))
            d = subset(d, valid_ratio != 0)

            return(d)
        },

        cache_path = function(exp_rel_path)
        {
            exp_rel_path = gsub('/', '', exp_rel_path)
            long_path = paste(exp_rel_path, collapse='-')
            cache_path = paste(digest(long_path), 'valid-ratios', 'rdata', sep='.')
            return(cache_path)
        },
        cache_exists = function(exp_rel_path)
        {
            path = cache_path(exp_rel_path)
            return( file.exists(path) )
        },
 
        fast_organize_ = function(d)
        {
            d = ddply(d, .(segment_bytes, 
                           filesystem, 
                           workload_class, 
                           appname,
                           testname,
                           expname,
                           blocksize), 
                      set_snake_points_for_a_snapshot)
            return(d)
        }
    )
)




 
c_______main_functions__________ <- function(){}

plot_set <- function(expname)
{
    trafficplot = TrafficSizePlot()
    validratioplot = ValidRatioPlot()
    gclogplot = GcLogPlot()
    cometgraph = CometGraph()
    workingset_plot = WorkingSetPlot()
    cometmovie = SubExpCometMovieAggregator()

    # trafficplot$run(expname)
    # validratioplot$run(expname)
    # gclogplot$run(expname)
    # cometgraph$run(expname)
    # workingset_plot$run(expname)
    cometmovie$run(expname)
}


plot_for_paper <- function()
{
    # The Appmix for Grouping section
    linegraph = AppMixSnakeGraph(
        rel_path=c(
                   'leveldb-fillseq-overwrite',
                   'sqlite-seqnrand',
                   'varmail-hotncold-allfs'
                   ))
    linegraph$plot_line_graph()

}


main <- function()
{
    # plot_for_paper()
    # return()

    linegraph = ValidRatioGraph()
    linegraph$plot_line_graph(
                                 exp_rel_path=c(
            # "grouping-rocksdb-tracesim2",
            # "grouping-leveldb-tracesim2",
            # "grouping-sqlitewal-tracesim2",
            # "grouping-sqliterb-tracesim2",
            # "grouping-varmail-tracesim2"

            # "grouping-rocksdb-tracesim4",
            # "grouping-leveldb-tracesim4",
            "rocks_n_level_grouping_no_oos",
            "grouping-sqlitewal-tracesim4",
            "grouping-sqliterb-tracesim4",
            "grouping-varmail-tracesim4"
               ))
 
    print_global_plots(save_to_file=F, w=20, h=20)
}

main()

