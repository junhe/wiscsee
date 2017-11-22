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
# setwd(WORKDIRECTORY)
sme <- function()
{
    WORKDIRECTORY= "/Users/junhe/workdir/analysis-script/"
    THISFILE     ='doraemon/tools.r'
    setwd(WORKDIRECTORY)
    source(THISFILE)
}


req_hist <- function(d, title)
{
    # d.global <<- d
    # print('saved')
    # return()

    d = d.global
    d = subset(d, operation == 'write')
    d = subset(d, offset > 750*MB/KB)
    d$phase = factor(findInterval(d$timestamp, seq(0, 50, 10)))

    p = ggplot(d, aes(x=size, fill=phase)) +
        geom_histogram(position='dodge') +
        ggtitle(title)
    print(p)
}

merge_requests <- function(d)
{
    d = arrange(d, operation, timestamp)
    n = nrow(d)

    d.new = NULL
    for ( i in seq(1, n) )
    {
        if (i == 1) {
            working_row = d[i, ]
            next
        }
        coming_row = d[i, ]
        if (working_row$offset + working_row$size == coming_row$offset &&
            working_row$operation == coming_row$operation ) 
        {
            working_row$size = working_row$size + coming_row$size 
        } else {
            d.new = rbind(d.new, working_row)
            working_row = coming_row
        }
    }
    d.new = rbind(d.new, working_row)

    return(d.new)
}



c_______TIME_SPACE_PLOT___________________ <- function() {}



Tools_SubExpTimeSpacePlot <- setRefClass("Tools_SubExpTimeSpacePlot",
    fields = list(subexppath="character", blocksize="numeric"),
    methods = list(
        run = function()
        {
            # load the objects
            confjson = create_configjson(subexppath=subexppath)
            blocksize <<- confjson$get_block_size()

            # events_obj = BlkParseEvents(filepath=paste(subexppath, 'blkparse-events-for-ftlsim-mkfs.txt', sep="/"))
            events_obj = BlkParseEvents(filepath=paste(subexppath, 'blkparse-events-for-ftlsim.txt', sep="/"))
            d.events = events_obj$load()
            print('i there')
            p = time_space_graph(d.events, title=confjson$get_filesystem())
            print(p)

            # subexpname = confjson$get_subexpname()
            # save_plot(p, subexpname, save=T, w=10, h=6)

            a = readline()
            if (a == 'q')
                stop()
        },
        time_space_graph = function(d, title='default title', 
            colorstr='operation', range=NA)
        {
            print('i am in unit')
            size_unit = MB
            d = transform(d, offset=offset/size_unit, size=size/size_unit)
            range = range/size_unit

            d = subset(d, action == 'D')

            d = subset(d, timestamp < 500000000)

            d = subset(d, timestamp > 0 & timestamp < 10)
            # d = subset(d, timestamp > 1 & timestamp < 1.1)
            # d = subset(d, offset >= 650 & offset < 675)
            # d = subset(d, offset > 50 & offset < 150)
            # d = subset(d, timestamp < 20.1)

            # d = subset(d, offset > 268 & offset < 269)
            # print(d)
            # d = arrange(d, timestamp)
            # d = transform(d, timestamp = seq_along(timestamp))

            # print(subset(d, operation == 'discard'))

            # print(quantile(d[d$operation == 'write',]$size*MB/KB))

            p = ggplot(d, aes_string(color=colorstr)) +
                geom_segment(aes(x=timestamp, xend=timestamp, y=offset, 
                                 yend=offset+size), size=5)+
                geom_point(aes(x=timestamp, y=offset)) +
                # scale_y_continuous(breaks=seq(0, 1024, 2)) +
                ylab('address') +
                # scale_x_continuous(breaks=seq(0, 100)) +
                # scale_y_continuous(breaks=seq(0, 2000, 100)) +
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
        }
    )
)





# original_Tools_SubExpTimeSpacePlot <- setRefClass("original_Tools_SubExpTimeSpacePlot",
    # fields = list(subexppath="character", blocksize="numeric"),
    # methods = list(
        # run = function()
        # {
            # # load the objects
            # confjson = create_configjson(subexppath=subexppath)
            # blocksize <<- confjson$get_block_size()

            # events_obj = BlkParseEvents(filepath=paste(subexppath, 'blkparse-events-for-ftlsim.txt', sep="/"))
            # d.events = events_obj$load()
            # p = time_space_graph(d.events)
            # print(p)

            # # subexpname = confjson$get_subexpname()
            # # save_plot(p, subexpname, save=T, w=10, h=6)
        # },
        # time_space_graph = function(d, title='default title', 
            # colorstr='operation', range=NA)
        # {
            # size_unit = MB
            # d = transform(d, offset=offset/size_unit, size=size/size_unit)
            # range = range/size_unit

            # # d = subset(d, timestamp < 500000000)

            # p = ggplot(d, aes_string(color=colorstr)) +
                # geom_segment(aes(x=timestamp, xend=timestamp, y=offset, 
                                 # yend=offset+size), size=5)+
                # geom_point(aes(x=timestamp, y=offset)) +
                # ylab('address') +
                # # xlab('seq id') +
                # ggtitle(title)

            # if (length(range) == 2) {
                # # d = subset(d, range[1] <= offset & offset <= range[2] )
                # padding = range[2] - range[1]
                # p = p + 
                    # coord_cartesian(
                        # ylim=c(range[1] - padding, range[2] + padding)) +
                    # geom_hline(y=range[1]) +
                    # geom_hline(y=range[2])
            # }

            # if (colorstr == 'operation') {
                # p = p + scale_color_manual(
                   # values=get_color_map2(c("read", "write", "discard")))
            # }
            # return(p)
        # }
    # )
# )



Tools_TimeSpaceBench <- setRefClass("Tools_TimeSpaceBench",
    fields = list(),
    methods = list(
        run = function(exp_rel_path)
        {
            print('i am in bench')
            subexpiter = SubExpIter(exp_rel_path=exp_rel_path)
            result = subexpiter$iter_each_subexp(Tools_SubExpTimeSpacePlot)
        }
    )
)


plot_space_vs_time_tools <- function(dirpaths)
{
    print('i am here')
    bench = Tools_TimeSpaceBench()
    bench$run(dirpaths)
}

c_______MERGED_TIME_VS_SPACE_IN_SEGS_____________ <- function()

Tools_SubExpTimeSpacePlot_Per_SEG <- setRefClass("Tools_SubExpTimeSpacePlot_Per_SEG",
    fields = list(subexppath="character", blocksize="numeric"),
    methods = list(
        run = function()
        {
            # load the objects
            confjson = create_configjson(subexppath=subexppath)
            blocksize <<- confjson$get_block_size()

            events_obj = BlkParseEvents(filepath=paste(subexppath, 'blkparse-events-for-ftlsim.txt', sep="/"))
            d.events = events_obj$load()

            for (segid in seq(0, 1024, 2)) {
                p = time_space_graph(d.events, title=confjson$get_filesystem(),
                                     range=c(segid*MB, (segid+2)*MB))

                if (is.null(p)) 
                    next
                subexpname = confjson$get_subexpname()
                subexpname = paste(segid)
                save_plot(p, subexpname, save=T, w=10, h=6)
                # print(p)

                # a = readline()
                # if (a == 'q')
                    # stop()
            }

        },
        time_space_graph = function(d, title='default title', 
            colorstr='operation', range=NA)
        {
            size_unit = MB
            d = transform(d, offset=offset/size_unit, size=size/size_unit)
            range = range/size_unit

            d = subset(d, timestamp < 500000000)
            # d = subset(d, timestamp > 39 & timestamp < 40)
            # d = subset(d, offset >= 678 & offset < 680)

            print('range')
            print(range)
            d = subset(d, offset >= range[1] & offset < range[2])

            if (nrow(d) == 0) {
                print(paste('no data in', range))
                return(NULL)
            }

            # d = subset(d, timestamp > 6.05 & timestamp < 6.06)
            # d = subset(d, offset > 750 & offset < 790)
            # d = subset(d, timestamp > 2 & timestamp < 2.3)
            # d = subset(d, offset > 100 & offset < 100.3)
            # d = subset(d, offset < 250 & offset > 30)

            d = merge_requests(d)
            print(d)

            d = arrange(d, timestamp)
            d = transform(d, timestamp = seq_along(timestamp))

            d = subset(d, timestamp < 20)

            p = ggplot(d, aes_string(color=colorstr)) +
                geom_segment(aes(x=timestamp, xend=timestamp, y=offset, 
                                 yend=offset+size), size=5)+
                # geom_text(aes(label=size*MB/KB, x=timestamp, y=offset))+
                geom_point(aes(x=timestamp, y=offset)) +
                # geom_hline(yintercept=128*5)+
                scale_y_continuous(breaks=seq(0, 2000, 2)) +
                ylab('address') +
                # expand_limits(y=0) +
                # xlab('seq id') +
                ggtitle(title)

            if (colorstr == 'operation') {
                p = p + scale_color_manual(
                   values=get_color_map2(c("read", "write", "discard")))
            }
            return(p)
        }
    )
)

Tools_TimeSpaceBench_Per_SEG <- setRefClass("Tools_TimeSpaceBench_Per_SEG",
    fields = list(),
    methods = list(
        run = function(exp_rel_path)
        {
            print('i am in bench')
            subexpiter = SubExpIter(exp_rel_path=exp_rel_path)
            result = subexpiter$iter_each_subexp(Tools_SubExpTimeSpacePlot_Per_SEG)
        }
    )
)



plot_space_vs_time_tools_per_seg <- function(dirpaths)
{
    print('i am here')
    bench = Tools_TimeSpaceBench_Per_SEG()
    bench$run(dirpaths)
}



c_______MERGED_REQ_SIZE_HIST_____________ <- function()

Tools_SubExpReqSizePlot <- setRefClass("Tools_SubExpReqSizePlot",
    fields = list(subexppath="character", blocksize="numeric"),
    methods = list(
        run = function()
        {
            # load the objects
            confjson = create_configjson(subexppath=subexppath)
            blocksize <<- confjson$get_block_size()

            events_obj = BlkParseEvents(filepath=paste(subexppath, 'blkparse-events-for-ftlsim.txt', sep="/"))
            d.events = events_obj$load()
            p = time_space_graph(d.events, title=confjson$get_filesystem())
            # print(p)

            subexpname = confjson$get_subexpname()

            d = arrange(d, operation, timestamp)
            d = merge_requests(d)

            req_hist(d, '')
            return()
        },
    )
)


Tools_MergedReqSizePlot <- setRefClass("Tools_MergedReqSizePlot",
    fields = list(),
    methods = list(
        run = function(exp_rel_path)
        {
            print('i am in bench')
            subexpiter = SubExpIter(exp_rel_path=exp_rel_path)
            result = subexpiter$iter_each_subexp(Tools_SubExpReqSizePlot)
        }
    )
)


c_______TIME_SPACE_PLOT___________________ <- function() {}

main <- function()
{
    plot_space_vs_time_tools( c(
        'traceit4'))

    # plot_space_vs_time_tools_per_seg(
                    # c(
                      # # 'sqlite-alignment-f2fs-no-ipu'
                    # # "varmail-noipu-8sec"
                    # # "f2fs-reclaim-test"
                      # )
                    # )


}

main()







