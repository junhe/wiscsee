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
    THISFILE     = 'doraemon/analyze-general.r'
    setwd(WORKDIRECTORY)
    source(THISFILE)
}

find_read_bw_start_time <- function(d.read)
{
    ratio = 0.5
    max_r_bw = max(d.read$read.bw)
    start_bw = max_r_bw * ratio

    start_timestamp = min(subset(d.read, read.bw > start_bw)$timestamp)

    return(start_timestamp)
}

 
c______imm_bw_____________ <- function(){}


SubExpSimBandwidth <- setRefClass("SubExpSimBandwidth",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            recjson <<- RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            bw = recjson$get_sim_write_bandwidth()

            return(list(
                    bw = bw,
                    appname = confjson$get_appmix_first_appname(),
                    filesystem = confjson$get_filesystem(),
                    FTL = confjson$get_ftl_type()
                    ))
        }
    )
)

SimBandwidth <- setRefClass("SimBandwidth",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_list_to_dataframe(
                   exp_rel_path, SubExpSimBandwidth)
            print(d)
            d = cols_to_num(d, c('bw'))

                 # bw appname filesystem
# 1  148.890092041148 LevelDB        xfs
# 2   149.51746635857 LevelDB        xfs
# 3    152.8442317916 LevelDB       f2fs

            levels(d$FTL) = plyr::revalue(levels(d$FTL), 
                c('dftldes'='Page', 'nkftl2'='Hybrid'))
            p = ggplot(d, aes(x=FTL, y=bw, fill=filesystem)) +
                geom_bar(stat='identity', position='dodge', width=0.7) +
                facet_grid(~appname) +
                scale_fill_manual( values=get_fs_color_map() ) +
                ylab('Bandwidth (MB/s)') +
                theme_zplot()
            print(p)
            save_plot(p, 'motivating', save=T, w=3.2, h=2.5)
        }
    )
)

 
c______sus_bw_____________ <- function(){}


SubExpSimBandwidthSus <- setRefClass("SubExpSimBandwidthSus",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            confjson <<- create_configjson(subexppath=subexppath)
            recjson <<- RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            user_traffic = recjson$get_user_traffic_snapshots()
            d = as.data.frame(user_traffic)

            d = arrange(d, timestamp)
            n = nrow(d)
            d$prev_size = c(NA, d$write_traffic_size[-n])
            d = transform(d, interval_size = write_traffic_size - prev_size)
            d = transform(d, bw = (interval_size/MB) / 0.1)

            trigger_time = recjson$get_gc_trigger_timestamp()


            d = transform(d, 
                    gc_trigger_timestamp = trigger_time,
                    appname = confjson$get_appmix_first_appname(),
                    filesystem = confjson$get_filesystem(),
                    FTL = confjson$get_ftl_type())

            d = d[-1, ]

            return(d)
        }
    )
)

SimBandwidthSus <- setRefClass("SimBandwidthSus",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(
                   exp_rel_path, SubExpSimBandwidthSus)
            
            # plot_ave_bw(d)
            # plot_bw_timeline(d)
            plot_ave_bw_before_gc(d)
        },
        plot_ave_bw_before_gc = function(d) {
            d = ddply(d, .(FTL, filesystem, appname),
                subset, timestamp <= gc_trigger_timestamp)

            d = aggregate(bw~FTL+filesystem+appname, data=d, mean)

            levels(d$FTL) = plyr::revalue(levels(d$FTL), 
                c('dftldes'='Page', 'nkftl2'='Hybrid'))
            p = ggplot(d, aes(x=FTL, y=bw, fill=filesystem)) +
                geom_bar(stat='identity', position='dodge', width=0.7) +
                facet_grid(~appname) +
                scale_fill_manual( values=get_fs_color_map() ) +
                ylab('Bandwidth (MB/s)') +
                theme_zplot()
 
            print(p)
        }, 
 
        plot_ave_bw = function(d) {
            # add the following 2 lines to only get average bw 
            # after the first gc
            # d = ddply(d, .(FTL, filesystem, appname),
                # subset, timestamp > gc_trigger_timestamp)

            d = aggregate(bw~FTL+filesystem+appname, data=d, mean)

            levels(d$FTL) = plyr::revalue(levels(d$FTL), 
                c('dftldes'='Page', 'nkftl2'='Hybrid'))
            p = ggplot(d, aes(x=FTL, y=bw, fill=filesystem)) +
                geom_bar(stat='identity', position='dodge', width=0.7) +
                facet_grid(~appname) +
                scale_fill_manual( values=get_fs_color_map() ) +
                ylab('Bandwidth (MB/s)') +
                theme_zplot()
 
            print(p)
        }, 
        plot_bw_timeline = function(d)
        {

            d = transform(d, yfacet = interaction(FTL, filesystem))

            d = arrange(d, timestamp)

            p = ggplot(d, aes(x=timestamp, y=bw, color=filesystem)) +
                geom_line() +
                geom_vline(aes(xintercept=gc_trigger_timestamp)) +
                facet_grid(yfacet~appname) +
                scale_fill_manual( values=get_fs_color_map() ) +
                ylab('Bandwidth (MB/s)') +
                theme_zplot()
            print(p)
 
        }
    )
)


SimBandwidth_IMM_N_SUS <- setRefClass("SimBandwidth_IMM_N_SUS",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path_imm, 
                        exp_rel_path_sus)
        {
            d.imm = aggregate_subexp_list_to_dataframe(
                   exp_rel_path_imm, SubExpSimBandwidth)
            d.imm = cols_to_num(d.imm, c('bw'))

            d.sus = aggregate_subexp_dataframes(
                   exp_rel_path_sus, SubExpSimBandwidthSus)
            d.sus = cols_to_num(d.sus, c('bw'))
            d.sus = aggregate(bw~FTL+filesystem+appname, data=d.sus, mean)

            print(head(d.imm))
            print(head(d.sus))

            d.imm = transform(d.imm, perf_type = 'Immediate')
            d.sus = transform(d.sus, perf_type = 'Sustainable')
            d = rbind(d.imm, d.sus)


            levels(d$FTL) = plyr::revalue(levels(d$FTL), 
                c('dftldes'='Page', 'nkftl2'='Hybrid'))
            p = ggplot(d, aes(x=FTL, y=bw, fill=filesystem)) +
                geom_bar(stat='identity', position='dodge', width=0.7) +
                facet_grid(perf_type~appname) +
                scale_fill_manual( values=get_fs_color_map() ) +
                ylab('Bandwidth (MB/s)') +
                theme_zplot() +
                theme(legend.margin = unit(0, 'cm'))
            print(p)
            save_plot(p, 'perf_imm_sus', save=T, w=3.2, h=2.5)
        }
    )
)

c______imm_sus_bw_from_long_run_____________ <- function(){}

# get all sus
# remove nkftl 1kb strip size
# calcuate imm and sus from there
SubExpSimBandwidthLONGRUN <- setRefClass("SubExpSimBandwidthLONGRUN",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            print(paste('SUB', subexppath))
            confjson <<- create_configjson(subexppath=subexppath)
            recjson <<- RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            user_traffic = recjson$get_user_traffic_snapshots()
            d = as.data.frame(user_traffic)
            

            d = calcuate_bw(d)

            trigger_time = recjson$get_gc_trigger_timestamp()
            if (is.null(trigger_time)) {
                # this effectively makes all current performance immediate
                trigger_time = 10000000
            }

            n_gc_procs = confjson$get_n_gc_procs()
            print(paste('gc procs', n_gc_procs))
            if (is.null(n_gc_procs)) {
                n_gc_procs = -1 
            }

            d = transform(d, 
                    gc_trigger_timestamp = trigger_time,
                    appname = confjson$get_testname_appname(),
                    testname = confjson$get_testname(),
                    rw=confjson$get_testname_rw(),
                    pattern=confjson$get_testname_pattern(),
                    filesystem = confjson$get_filesystem(),
                    FTL = confjson$get_ftl_type(),
                    n_channels = confjson$get_n_channels_per_dev(),
                    n_gc_procs = n_gc_procs,
                    over_provisioning = confjson$get_over_provisioning()
                    )

            d = d[-1, ]

            d = remove_aging_bw_for_read(d)

            return(d)
        },
        calcuate_bw = function(d)
        {
            d = arrange(d, timestamp)
            n = nrow(d)

            d$prev_write_traffic_size = c(NA, d$write_traffic_size[-n])
            d = transform(d, write_interval_size = write_traffic_size - prev_write_traffic_size)
            d = transform(d, write.bw = (write_interval_size/MB) / 0.1)

            d$prev_read_traffic_size = c(NA, d$read_traffic_size[-n])
            d = transform(d, read_interval_size = read_traffic_size - prev_read_traffic_size)
            d = transform(d, read.bw = (read_interval_size/MB) / 0.1)

            return(d)
        },
        remove_aging_bw_for_read = function(d)
        {
            d.read = subset(d, rw == 'r')
            d.write = subset(d, rw == 'w')

            read_bw_start_timestamp = find_read_bw_start_time(d.read)

            d.read = subset(d.read, timestamp > read_bw_start_timestamp) 

            d = rbind(d.read, d.write)
            return(d)
        }
    )
)


LongRunImmSusBwPlot <- setRefClass("LongRunImmSusBwPlot",
    fields = list(expname='character'),
    contains = c('PlotWithCache'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = get_data(exp_rel_path, classname='integration-bw', try_cache=F)

            d = transform(d, perf_type = factor(perf_type))

            do_bw_plot(d, rw.sel='w', FTL.sel='dftldes')
            do_bw_plot(d, rw.sel='r', FTL.sel='dftldes')

            do_bw_plot(d, rw.sel='w', FTL.sel='nkftl2')
            do_bw_plot(d, rw.sel='r', FTL.sel='nkftl2')
        },
        do_bw_plot = function(d, rw.sel, FTL.sel)
        {

            if (rw.sel == 'w') {
                bw.type.sel = 'write.bw'
            } else if (rw.sel == 'r') {
                bw.type.sel = 'read.bw'
            }

            n_channels = unique(d$n_channels)
            title = paste(rw.sel, bw.type.sel, FTL.sel, n_channels)
            print(title)

            d = subset(d, rw == rw.sel & bw.type == bw.type.sel & FTL == FTL.sel)

            if (nrow(d) == 0)  {
                print(paste('skip', title))
                return()
            } else {
                print(paste('doing', title))
            }

            d = shorten_appnames(d)
            d = rename_pattern_levels(d)
            d = order_fs_levels(d)

            p = ggplot(d, aes(x=filesystem, y=bw, fill=perf_type)) +
                geom_bar(stat='identity', position='dodge') +
                geom_text(aes(label=round(bw)), position=position_dodge(1), size=3,
                          angle=90, hjust=-0.1
                          ) +
                scale_fill_manual(values=get_perf_type_color_map()) +
                facet_grid(pattern~appname) +
                theme_zplot() + 
                ggtitle(title)

            print(p)
            save_plot(p, title, save=T, w=8, h=6)
        },
        get_raw_data = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(
                   exp_rel_path, SubExpSimBandwidthLONGRUN)

            d = transform(d, is_imm = timestamp <= gc_trigger_timestamp)
            d$perf_type = 'Sustainable'
            if (length(d[d$is_imm, ]$perf_type) > 0) {
                d[d$is_imm, ]$perf_type = 'Immediate'
            }

            d = melt(d, id=c("FTL", "rw", "pattern", "FTL", 
                             "filesystem", "appname", "perf_type", 
                             "n_channels", "over_provisioning"),
                        measure=c('write.bw', 'read.bw'), value.name='bw')

            d = plyr::rename(d, c('variable'='bw.type'))

            # for rw == 'w', keep write.bw
            # for rw == 'r', keep read.bw
            d = subset(d, (rw == 'w' & bw.type == 'write.bw') | 
                          (rw == 'r' & bw.type == 'read.bw'))

            # Read does not have immediate and sustainable
            if (length(d[d$rw == 'r', ]$perf_type) > 0) {
                d[d$rw == 'r', ]$perf_type = 'Regular.Read'
            }

            d = aggregate(bw~rw+FTL+pattern+FTL+filesystem+
                  over_provisioning+appname+perf_type+bw.type+n_channels, data=d, mean)

            return(d)
        }
    )
)

LongRunImmSusBwPlotFOCUS <- setRefClass("LongRunImmSusBwPlotFOCUS",
    fields = list(expname='character'),
    contains = c('PlotWithCache'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = get_data(exp_rel_path, classname='integration-bw', try_cache=T)

            d = subset(d, appname == 'sqliteRB' | appname == 'sqliteWAL')
            d = subset(d, pattern == 'rand')

            d = transform(d, perf_type = factor(perf_type))

            do_bw_plot(d, 'r')
            do_bw_plot(d, 'w')
        },
        do_bw_plot = function(d, rw.sel)
        {
            if (rw.sel == 'w') {
                bw.type.sel = 'write.bw'
            } else if (rw.sel == 'r') {
                bw.type.sel = 'read.bw'
            }

            n_channels = unique(d$n_channels)
            title = paste(rw.sel, bw.type.sel, n_channels, sep='-')
            print(title)

            print(head(d))

            d = subset(d, rw == rw.sel & bw.type == bw.type.sel)

            if (nrow(d) == 0)  {
                print(paste('skip', title))
                return()
            } else {
                print(paste('doing', title))
            }

            d = shorten_appnames(d)
            d = rename_pattern_levels(d)
            d = order_fs_levels(d)
            d = rename_ftlname(d)

            d = calcuate_bw_utilization(d)

            print(head(d))
            d = transform(d, xfacet = interaction(FTL, 
                              n_gc_procs, over_provisioning, sep='-', lex.order=T))
            # d = transform(d, xfacet = FTL)

            d = rename_xfacet(d)

            p = ggplot(d, aes(x=filesystem, y=bw, fill=perf_type)) +
                geom_bar(stat='identity', position='dodge', width=0.7) +
                # geom_text(aes(label=round(bw)), position=position_dodge(1), size=3,
                          # angle=90, hjust=-0.1
                          # ) +
                # scale_y_continuous(limits=c(0, 1200)) +
                scale_fill_manual(values=get_perf_type_color_map()) +
                facet_grid(appname~xfacet) +
                theme_zplot() +
                theme(axis.title.x = element_blank()) +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))

            if (rw.sel == 'r') {
                p = p + theme(legend.position="none") +
                        scale_y_continuous(breaks=seq(0, 2000, 100))+
                        ylab('Bandwidth (MB/s)') +
                        ggtitle('Read Performance')

                save_plot(p, paste('integration', rw.sel, sep='-'), 
                          save=T, w=3.3/2, h=2)
            } else {
                d_arrow = subset(d, 
                                 xfacet %in% c('Pg', 'PgLim') 
                                 & filesystem == 'f2fs' 
                                 & perf_type == 'Sustainable' & appname == 'sqRB')
                d_arrow = transform(d_arrow, xx = 2.22, xxend = 2.22,
                                    yy=50, yyend=max(bw)
                                    )
                print(d_arrow)

                p = p + geom_segment(data=d_arrow, aes(x=xx, xend=xxend,
                        y=yy, yend=yyend),
                        size = 0.2,
                        arrow=arrow(length = unit(0.1, "npc"))
                        )

                p = p + theme(legend.position=c(0.5, 0.45)) +
                        theme(legend.key.size = unit(0.2, 'cm')) +
                      theme(legend.text = element_text(size = 5)) +
                      theme(legend.background = element_blank())
                p = p + ylab('Bandwidth (MB/s)') +
                        ggtitle('Write Performance')


                save_plot(p, paste('integration', rw.sel, sep='-'), 
                          save=T, w=3.3/2, h=2)
            }

            print(p)
        },
        rename_xfacet = function(d)
        {
            lvs = levels(d$xfacet)
            lvs = gsub( paste('Page', -1, 1.5, sep='-'), 'Pg', lvs, fixed=T)
            lvs = gsub( paste('Page', 1, 1.5, sep='-'), 'PgLim', lvs, fixed=T)
            lvs = gsub( paste('Hybrid', -1, 1.5, sep='-'), 'Hybrid', lvs, fixed=T)
            levels(d$xfacet) = lvs

            return(d)
        },
        calcuate_bw_utilization = function(d)
        {
            channel_r_max = 78.125
            channel_w_max = 9.765625
            d = transform(d, r_max_bw = channel_r_max * n_channels)
            d = transform(d, w_max_bw = channel_w_max * n_channels)
            d = transform(d, max_bw = ifelse(rw == 'r', r_max_bw, w_max_bw))

            d = transform(d, bw_util =bw / max_bw)
            return(d)
        },
        get_raw_data = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(
                   exp_rel_path, SubExpSimBandwidthLONGRUN)

            d = transform(d, is_imm = timestamp <= gc_trigger_timestamp)
            d$perf_type = 'Sustainable'
            if (length(d[d$is_imm, ]$perf_type) > 0) {
                d[d$is_imm, ]$perf_type = 'Immediate'
            }

            d = melt(d, id=c("FTL", "rw", "over_provisioning", "pattern", "FTL", "n_gc_procs",
                             "filesystem", "appname", "perf_type", "n_channels"),
                        measure=c('write.bw', 'read.bw'), value.name='bw')

            d = plyr::rename(d, c('variable'='bw.type'))

            # for rw == 'w', keep write.bw
            # for rw == 'r', keep read.bw
            d = subset(d, (rw == 'w' & bw.type == 'write.bw') | 
                          (rw == 'r' & bw.type == 'read.bw'))

            # Read does not have immediate and sustainable
            if (length(d[d$rw == 'r', ]$perf_type) > 0) {
                d[d$rw == 'r', ]$perf_type = 'Regular.Read'
            }

            d = aggregate(bw~rw+FTL+over_provisioning+n_gc_procs+
                  pattern+FTL+filesystem+appname+perf_type+bw.type+n_channels, data=d, mean)

            return(d)
        }
    )
)


 
LongRunImmSusBwTimelinePlot <- setRefClass("LongRunImmSusBwTimelinePlot",
    fields = list(expname='character'),
    contains = c('PlotWithCache'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = get_data(exp_rel_path, classname='integration', try_cache=F)

            d = order_appname_levels(d)
            apps = unique(d$appname)
            rwmodes = unique(d$rw)

            for (app in apps) {
                for (rwmode in rwmodes) {
                    dd = subset(d, appname == app & rw == rwmode) 
                    plot_one_app(dd)
                    # a = readline()
                    # if (a == 'q')
                        # stop()
                }
            }
        },
        plot_one_app = function(d)
        {
            rwmode = paste(unique(d$rw))
            app = paste(unique(d$appname))
            n_channels = unique(d$n_channels)
            title = paste(app, rwmode, n_channels)

            d = order_pattern_levels(d)

            d.end = aggregate(timestamp~filesystem+pattern+FTL, data=d, max)

            if (rwmode == 'r') {
                d = transform(d, bw = read.bw)
            } else {
                d = transform(d, bw = write.bw)
            }

            p = ggplot(d, aes(x=timestamp, y=bw, color=filesystem)) +
                geom_line(alpha=0.8) +
                # geom_vline(data=d.end, aes(xintercept=timestamp, color=filesystem)) + 
                geom_text(data=d.end, aes(x=timestamp, y=150, label=filesystem)) +
                facet_grid(pattern~FTL) +
                ggtitle(title)
            print(p)
            save_plot(p, title, save=T, w=15, h=12, ext='png')
        },
        get_raw_data = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(
                   exp_rel_path, SubExpSimBandwidthLONGRUN)

            return(d)
        }
    )
)



c_____Average_Req_Size______________ <- function(){}

SubExpBlkEvents <- setRefClass("SubExpBlkEvents",
    fields = list(subexppath="character", confjson='ConfigJson', 
                  recjson='RecorderJson'),
    methods = list(
        run = function()
        {
            print(subexppath)
            confjson <<- create_configjson(subexppath=subexppath)
            blk_events = BlkParseEvents(
                  filepath=paste(subexppath, 'blkparse-events-for-ftlsim.txt', sep='/'))

            d = blk_events$load()
            d = subset(d, action == 'D')
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

            d = aggregate(size~testname+appname+filesystem+rw+pattern+operation, data=d, mean)

            return(d)
        }
    )
)

Avg_Req_Size <- setRefClass("Avg_Req_Size",
    fields = list(expname='character'),
    contains = c('PlotWithCache'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = get_data(exp_rel_path, classname='avg-req-size-quick')

            do_quick_plot(d)
        },
        get_raw_data = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpBlkEvents)

           return(d)
        },
        do_quick_plot = function(d)
        {
            d = transform(d, size = size / KB)
            d = order_fs_levels(d)
            d = order_pattern_levels(d)
            d = order_rw_levels(d)
 
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
            print(p)
            save_plot(p, 'avg-req-size', save=T, w=10, h=6)
        }
    )
)


c_______________plot_ncq_table_______________________ <- function(){}

plot_ncq_median <- function()
{
    d = read.table('/Users/junhe/datahouse/localresults/ncqtable/ncqtable.txt',
                   header = T)
    print(d) 

    d = order_pattern_levels(d)
    p = ggplot(d, aes(x=pattern, y=median, fill=fs)) + 
        geom_bar(stat='identity', position='dodge') +
        geom_text(aes(label=round(median)), position=position_dodge(1), size=3,
                  angle=90, hjust=-0.1
                  ) +
        # coord_cartesian(xlim=c(0, 2000)) +
        ylab('Median Depth') +
        scale_fill_manual(values=get_fs_color_map()) +
        expand_limits(y=0) +
        facet_grid(rw~appname, scales='free_x', space='free_x') +
        theme_zplot() +
        theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
        theme(axis.title.x=element_blank())
    print(p)
}

c______________________________________ <- function(){}

batch_plot_raw <- function(exp_rel_path)
{

    ######## for explore
    avgsize = Avg_Req_Size()
    avgsize$plot(exp_rel_path)

}


batch_plot_integration <- function(exp_rel_path)
{
    # bwtimelineplot = LongRunImmSusBwTimelinePlot()
    # bwtimelineplot$plot(exp_rel_path)

    # bw = LongRunImmSusBwPlot()
    # bw$plot(exp_rel_path)

    bw_focus = LongRunImmSusBwPlotFOCUS()
    bw_focus$plot(exp_rel_path)
}


main <- function()
{
    ###############################################################
    # this one has 16 channels
    batch_plot_integration(
       c(
         'integration-median-depths-try2', 
         'integration-16channle-sqliterb-rand-w-nkftl-makeup',
         'integration-1-gc-proc-really-1'

         # 'integration-1.2provision',
         # 'integration-2.0provision'
         # 'integration-1.1provision-1024cleaners',
         # 'integration-1.1provision-1cleaners'
         ) )
    # batch_plot_integration('integration-median-depths-4-channels')
    # batch_plot_integration('integration-median-depths-8-channels')
    # batch_plot_integration('integration-median-depths-inf-stop')

    # batch_plot_raw(c(
                 # 'leveldb-longer-for-gc-sorttrace',
                 # 'rocksdb-longer-for-gc-sorttrace2',
                 # 'sqlitewal-longer-for-gc-sorttrace',
                 # 'sqliterb-longer-for-gc-sorttrace',
                 # 'varmail-reqscale-002'
                 # ))

    # batch_plot_raw(c(
     # 'leveldb-longer-for-gc-sorttrace/subexp--6835925747727223284-ext4-10-17-22-27-24-4373209935084946648',
     # 'leveldb-longer-for-gc-sorttrace/subexp--7948252794759332065-ext4-10-17-22-33-19-2159710253658138684'
     # ))


    # plot_ncq_median()
}

main()

