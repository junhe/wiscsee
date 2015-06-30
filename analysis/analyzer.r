# libraries
library(ggplot2)
library(plyr)
library(reshape2)
library(gridExtra)

# copy the following so you can do sme()
WORKDIRECTORY= "/Users/junhe/BoxSync/0-MyResearch/Doraemon/workdir/doraemon/analysis"
THISFILE     ='analyzer.r'
setwd(WORKDIRECTORY)
sme <- function()
{
    setwd(WORKDIRECTORY)
    source(THISFILE)
}

explore.FSJ386323 <- function()
{
    transfer <- function()
    {
    }
    load <- function()
    {
        d = read.csv('./myresult2', header=T, sep=';')
        return(d)
    }

    clean <- function(d)
    {
        return(d)
    }

    func <- function(d)
    {
        d$blockstart = d$blockstart*512
        d$blockstar = factor(d$blockstart)
        print(summary(d))
        levels(d$blockstart) = seq_along(levels(d$blockstart))
        p = ggplot(d,aes(x=time, y=blockstart)) +
            # geom_linerange(aes(x=time, ymin=blockstart, ymax=blockstart+size),
                           # size=5) 
            geom_point()
        print(p)
    }

    do_main <- function()
    {
        d = load()
        d = clean(d)
        func(d)
    }
    do_main()
}


explore.FIU <- function()
{
    transfer <- function()
    {
    }
    load <- function(fpath)
    {
        d = read.table(fpath, header=F, 
           col.names = c('timestamp', 'pid', 'cmd', 'blocknum',  
                         'size', 'action', 'x', 'y', 'z'),
                       )
        d$x = NULL
        d$y = NULL
        d$z = NULL
        return(d)
    }

    clean <- function(d)
    {
        return(d)
    }

    func <- function(d)
    {
        print(head(d))
        print(summary(d))

        d$blocknum = d$blocknum * 512/2^30
        p = ggplot(d, aes(x=timestamp, y=blocknum)) +
            geom_point()
        print(p)
        return(p)
    }

    do_main <- function()
    {
        myfiles = list.files('./madmax')
        for (f in myfiles) {
            ff = paste('./madmax/', f, sep='')
            print(ff)
            d <- load(ff)
            d = clean(d)
            p = func(d)
            ggsave(paste(f, '.pdf', sep=''), plot=p)
            # a = readline()
            # if (a == 'a')
                # break
        }
    }
    do_main()
}


load_file_from_cache <- function(fpath, reader.funcname)
{
    # This function creates a cache using memory
    # if fpath is not in memory, it reads the file
    # into a data frame and return it. 
    # if fpath is in memory, it simply return it
    
    # reader.funcname is the name of the function used to 
    # read fpath to a data frame, it takes fpath as the only
    # parameter
    # Example
    # 
    # myreader <- function(fpath)
    # {
    #     return(1000)   
    # }
    # 
    # Note that the function is passed in as string
    # load_file_from_cache('file03', 'myreader')
    
    if ( exists('filecache') == FALSE ) {
        print('filecache not exist')
        filecache <<- list()
    } else {
        print('filecache exist')
    }
    
    if ( exists(fpath, where=filecache) == TRUE ) {
        print('in cache')
        return(filecache[[fpath]])
    } else {
        # not in cache
        print('not in cache')
        f = match.fun(reader.funcname)
        d = f(fpath)
        filecache[[fpath]] <<- d
        return(d)
    }
    
}


# Sort levels of col1 by col2
sort_levels <- function(d, col1, col2)
{
    tmpfactor = as.character(d[, col1])
    # order tmpfactor by col2
    tmpfactor = tmpfactor[ order(d[, col2]) ]
    # get new sorted levels
    newlevels = unique(tmpfactor)
    # make a new factor
    d[, col1] = factor(d[, col1], levels=newlevels)

    return(d)
}


explore.madmax.1.blkparse <- function()
{
    transfer <- function()
    {
    }
    load <- function(fpath)
    {
        d = read.table(fpath, header=F, 
           col.names = c('timestamp', 'pid', 'cmd', 'blocknum',  
                         'size', 'action', 'x', 'y', 'z'),
                       )
        d$x = NULL
        d$y = NULL
        d$z = NULL
        return(d)
    }

    clean <- function(d)
    {
        return(d)
    }

    func <- function(d)
    {
        # who issued the most requests
        count_cmd <- function(d)
        {
            cnt = as.data.frame(table(d$cmd))
            # cnt = arrange(cnt, Freq)
            cnt = sort_levels(cnt, 'Var1', 'Freq')
            print(cnt)

            p = ggplot(cnt, aes(x=Var1, y=Freq)) +
                geom_bar(stat='identity') +
                coord_flip()
            # print(p)
            return(p)
        }


        blocknum.time.plot <- function(d) 
        {
            d$blocknum = d$blocknum * 512/2^30
            p = ggplot(d, aes(x=timestamp, y=blocknum)) +
                geom_point()
            # print(p)
            return(p)
        }

        hot.addresses <- function(d)
        {
            d$blocknum = d$blocknum * 512/2^30
            p = ggplot(d, aes(x=blocknum)) +
                geom_histogram() +
                xlab('address (GB)')
            # print(p)
            return(p)
        }

        p1 = count_cmd(d)
        p2 = blocknum.time.plot(d)
        p3 = hot.addresses(d)
        grid.arrange(p1, p2, p3)
        return()
    }

    do_main <- function()
    {
        f = c("./data/madmax/madmax-110108-112108.1.blkparse")
        # d <<- load(f)
        d = clean(d)
        p = func(d)
    }
    do_main()
}

explore.madmax.iterate <- function()
{
    transfer <- function()
    {
    }
    load <- function(fpath)
    {
        d = read.table(fpath, header=F, 
           col.names = c('timestamp', 'pid', 'cmd', 'blocknum',  
                         'size', 'action', 'x', 'y', 'z'),
                       )
        d$x = NULL
        d$y = NULL
        d$z = NULL
        return(d)
    }

    clean <- function(d)
    {
        return(d)
    }

    func <- function(d)
    {
        # who issued the most requests
        count_cmd <- function(d)
        {
            cnt = as.data.frame(table(d$cmd))
            # cnt = arrange(cnt, Freq)
            cnt = sort_levels(cnt, 'Var1', 'Freq')
            print(cnt)

            p = ggplot(cnt, aes(x=Var1, y=Freq)) +
                geom_bar(stat='identity') +
                coord_flip()
            # print(p)
            return(p)
        }


        blocknum.time.plot <- function(d) 
        {
            # find top 5 cmds
            cnt = as.data.frame(table(d$cmd))
            cnt = arrange(cnt, Freq)
            tops = tail(cnt$Var1, 5)

            d = subset(d, cmd %in% tops)

            d$blocknum = d$blocknum * 512/2^30
            p = ggplot(d, aes(x=timestamp, y=blocknum, color=cmd)) +
                geom_point()
            # print(p)
            return(p)
        }

        hot.addresses <- function(d)
        {
            d$blocknum = d$blocknum * 512/2^30
            p = ggplot(d, aes(x=blocknum)) +
                geom_histogram() +
                xlab('address (GB)')
            # print(p)
            return(p)
        }

        p1 = count_cmd(d)
        p2 = blocknum.time.plot(d)
        p3 = hot.addresses(d)
        return(list(p1, p2, p3))
    }

    do_main <- function()
    {
        dirpath = "./data/madmax/"
        files = list.files(dirpath)
        plist = list()
        for ( f in files ) {
            f = paste(dirpath, f, sep='')
            d <- load(f)
            d = clean(d)
            ps = func(d)
            plist = append(plist, ps)
            a = readline()
            if ( a == 'a')
                break
        }
        do.call('grid.arrange', c(plist, ncol=1))
    }
    do_main()
}

explore.mail01 <- function()
{
    transfer <- function()
    {
    }
    load <- function(fpath)
    {
        d = read.table(fpath, header=F, 
           col.names = c('timestamp', 'pid', 'cmd', 'blocknum',  
                         'size', 'action', 'x', 'y', 'z'),
                       )
        d$x = NULL
        d$y = NULL
        d$z = NULL
        return(d)
    }

    clean <- function(d)
    {
        return(d)
    }

    func <- function(d)
    {
        # who issued the most requests
        count_cmd <- function(d)
        {
            cnt = as.data.frame(table(d$cmd))
            # cnt = arrange(cnt, Freq)
            cnt = sort_levels(cnt, 'Var1', 'Freq')
            print(cnt)

            p = ggplot(cnt, aes(x=Var1, y=Freq)) +
                geom_bar(stat='identity') +
                coord_flip()
            # print(p)
            return(p)
        }


        blocknum.time.plot <- function(d) 
        {
            # find top 5 cmds
            cnt = as.data.frame(table(d$cmd))
            cnt = arrange(cnt, Freq)
            tops = tail(cnt$Var1, 5)

            d = subset(d, cmd %in% tops)

            d$blocknum = d$blocknum * 512/2^30
            p = ggplot(d, aes(x=timestamp, y=blocknum, color=cmd)) +
                geom_point(size=1) +
                expand_limits(y=0) +
                ylab('LBA (GB)')
            # print(p)
            return(p)
        }

        hot.addresses <- function(d)
        {
            d$blocknum = d$blocknum * 512/2^30
            p = ggplot(d, aes(x=blocknum)) +
                geom_histogram() +
                xlab('LBA (GB)') +
                expand_limits(x=0) +
                coord_flip()
            # print(p)
            return(p)
        }

        p1 = count_cmd(d)
        # p2 = blocknum.time.plot(d)
        # p3 = hot.addresses(d)
        return(list(p1, p2, p3))
    }

    do_main <- function()
    {
        dirpath = "./data/mail-01/"
        files = list.files(dirpath, pattern='10-million')
        # files = list.files(dirpath, pattern='sample')
        print(files)
        plist = list()
        for ( f in files ) {
            f = paste(dirpath, f, sep='')
            # d <- load(f)
            d <- load_file_from_cache(f, 'load')
            d = clean(d)
            ps = func(d)
            plist = append(plist, ps)
            # a = readline()
            # if ( a == 'a')
                # break
        }
        do.call('grid.arrange', c(plist, ncol=1))
    }

    count_block_accesses <- function(d)
    {
        cnt = as.data.frame(table(d$blocknum))
        freqdist = as.data.frame(table(cnt$Freq))
        print(summary(freqdist))
        freqdist$Var1 = as.numeric(freqdist$Var1)

        p1 = ggplot(freqdist, aes(x=Var1, y=log10(Freq))) +
            geom_point() +
            geom_line() +
            xlab('x is access count, y is number of blocks having this count')


        p2 = ggplot(cnt, aes(x=Freq)) +
            stat_ecdf() +
            xlab('access count for each block') +
            scale_y_continuous(breaks=seq(0, 1, 0.1)) +
            scale_x_continuous(breaks=seq(0, 5000, 100)) + 
            theme(axis.text.x = element_text(angle=90)) 

        p3 = ggplot(cnt, aes(x=Freq)) +
            stat_ecdf() +
            xlab('access count for each block') +
            scale_y_continuous(breaks=seq(0, 1, 0.1)) +
            # scale_x_continuous(breaks=seq(0, 5000, 100)) + 
            scale_x_continuous(breaks=seq(0, 10, 1)) + 
            theme(axis.text.x = element_text(angle=90)) +
            coord_cartesian(xlim = c(0, 10))
        
        return(list(p1, p2, p3))
    }

    do_main2 <- function()
    {
        dirpath = "./data/mail-01/"
        files = list.files(dirpath, pattern='10-million')
        # files = list.files(dirpath, pattern='sample')
        print(files)
        plist = list()
        for ( f in files ) {
            f = paste(dirpath, f, sep='')
            d <- load_file_from_cache(f, 'load')
            d = clean(d)
            plots = count_block_accesses(d)
            plist = append(plist, plots)
        }
        do.call('grid.arrange', c(plist, ncol=1))
    }
 
    do_main3 <- function()
    {
        dirpath = "./data/mail-01/"
        files = list.files(dirpath, pattern='10-million')
        # files = list.files(dirpath, pattern='sample')
        for ( f in files ) {
            f = paste(dirpath, f, sep='')
            d <- load_file_from_cache(f, 'load')
            d = clean(d)
            print(count(d$size))
        }
    }
  
    do_main_vis_lives <- function()
    {
        ddply_timeextent <- function(d, globaltimeend)
        {
            tmpend = c(d$timestamp[-1], globaltimeend)
            d$endtimestampe = tmpend
            d$reincarnation = seq_along(d$timestamp)
            return(d)
        }

        plot_life <- function(d)
        {
            d$blocknum = as.factor(d$blocknum)
            p = ggplot(d, aes(x=timestamp, xend=endtimestampe,
                              y=blocknum,  yend=blocknum, color=reincarnation)) +
                geom_segment()
            # print(p)
            ggsave(file='mail01-lives-1-30000.pdf', p, w=6, h=100, limitsize=FALSE)
        }

        dirpath = "./data/mail-01/"
        files = list.files(dirpath, pattern='1-million')
        for ( f in files ) {
            f = paste(dirpath, f, sep='')
            d <- load_file_from_cache(f, 'load')
            d = clean(d)

            d = d[1:30000, ]
            globaltimeend = tail(d$timestamp, 1)
            d = ddply(d, .(blocknum), ddply_timeextent, globaltimeend)
            d$reincarnation = factor(d$reincarnation)
            d$reincarnation = factor(d$reincarnation, levels=sample(levels(d$reincarnation)))
            plot_life(d)
            print('done')

            a = readline()
            if ( a == 'a')
                break
        }
    }

    # do_main()
    # do_main2()
    # do_main3()
    do_main_vis_lives()
}


explore.websearch <- function()
{
    transfer <- function()
    {
    }
    load <- function(fpath)
    {
        d = read.table(fpath, header=F, 
           col.names = c('timestamp', 'pid', 'cmd', 'blocknum',  
                         'size', 'action', 'x', 'y', 'z'),
                       )
        d$x = NULL
        d$y = NULL
        d$z = NULL
        return(d)
    }

    clean <- function(d)
    {
        return(d)
    }

    func <- function(d)
    {
        # who issued the most requests
        count_cmd <- function(d)
        {
            cnt = as.data.frame(table(d$cmd))
            # cnt = arrange(cnt, Freq)
            cnt = sort_levels(cnt, 'Var1', 'Freq')
            print(cnt)

            p = ggplot(cnt, aes(x=Var1, y=Freq)) +
                geom_bar(stat='identity') +
                coord_flip()
            # print(p)
            return(p)
        }


        blocknum.time.plot <- function(d) 
        {
            # find top 5 cmds
            cnt = as.data.frame(table(d$cmd))
            cnt = arrange(cnt, Freq)
            tops = tail(cnt$Var1, 5)

            d = subset(d, cmd %in% tops)

            d$blocknum = d$blocknum * 512/2^30
            p = ggplot(d, aes(x=timestamp, y=blocknum, color=cmd)) +
                geom_point(size=1) +
                expand_limits(y=0) +
                ylab('LBA (GB)')
            # print(p)
            return(p)
        }

        hot.addresses <- function(d)
        {
            d$blocknum = d$blocknum * 512/2^30
            p = ggplot(d, aes(x=blocknum)) +
                geom_histogram() +
                xlab('LBA (GB)') +
                expand_limits(x=0) +
                coord_flip()
            # print(p)
            return(p)
        }

        p1 = count_cmd(d)
        p2 = blocknum.time.plot(d)
        p3 = hot.addresses(d)
        return(list(p1, p2, p3))
    }

    do_main <- function()
    {
        dirpath = "./data/webresearch/"
        files = list.files(dirpath, pattern='blkparse')
        # files = list.files(dirpath, pattern='sample')
        print(files)
        plist = list()
        for ( f in files ) {
            f = paste(dirpath, f, sep='')
            d <- load_file_from_cache(f, 'load')
            d = clean(d)
            ps = func(d)
            plist = append(plist, ps)
            # a = readline()
            # if ( a == 'a')
                # break
        }
        do.call('grid.arrange', c(plist, ncol=1))
    }

    count_block_accesses <- function(d)
    {
        cnt = as.data.frame(table(d$blocknum))
        freqdist = as.data.frame(table(cnt$Freq))
        print(summary(freqdist))
        freqdist$Var1 = as.numeric(freqdist$Var1)

        p1 = ggplot(freqdist, aes(x=Var1, y=log10(Freq))) +
            geom_point() +
            geom_line() +
            xlab('x is access count, y is number of blocks having this count')


        p2 = ggplot(cnt, aes(x=Freq)) +
            stat_ecdf() +
            xlab('access count for each block') +
            scale_y_continuous(breaks=seq(0, 1, 0.1)) +
            scale_x_continuous(breaks=seq(0, 5000, 100)) + 
            theme(axis.text.x = element_text(angle=90)) 

        p3 = ggplot(cnt, aes(x=Freq)) +
            stat_ecdf() +
            xlab('access count for each block') +
            scale_y_continuous(breaks=seq(0, 1, 0.1)) +
            # scale_x_continuous(breaks=seq(0, 5000, 100)) + 
            scale_x_continuous(breaks=seq(0, 10, 1)) + 
            theme(axis.text.x = element_text(angle=90)) +
            coord_cartesian(xlim = c(0, 10))
        
        return(list(p1, p2, p3))
    }

    do_main2 <- function()
    {
        dirpath = "./data/mail-01/"
        files = list.files(dirpath, pattern='10-million')
        # files = list.files(dirpath, pattern='sample')
        print(files)
        plist = list()
        for ( f in files ) {
            f = paste(dirpath, f, sep='')
            d <- load_file_from_cache(f, 'load')
            d = clean(d)
            plots = count_block_accesses(d)
            plist = append(plist, plots)
        }
        do.call('grid.arrange', c(plist, ncol=1))
    }
 
    do_main3 <- function()
    {
        dirpath = "./data/mail-01/"
        files = list.files(dirpath, pattern='10-million')
        # files = list.files(dirpath, pattern='sample')
        for ( f in files ) {
            f = paste(dirpath, f, sep='')
            d <- load_file_from_cache(f, 'load')
            d = clean(d)
            print(count(d$size))
        }
    }
 
    do_main_vis_lives <- function()
    {
        ddply_timeextent <- function(d, globaltimeend)
        {
            tmpend = c(d$timestamp[-1], globaltimeend)
            d$endtimestampe = tmpend
            d$reincarnation = seq_along(d$timestamp)
            return(d)
        }

        plot_life <- function(d)
        {
            d$blocknum = as.factor(d$blocknum)
            p = ggplot(d, aes(x=timestamp, xend=endtimestampe,
                              y=blocknum,  yend=blocknum, color=reincarnation)) +
                geom_segment()
            # print(p)
            ggsave(file='websearch-lives.pdf', p, w=6, h=80, limitsize=FALSE)
        }

        dirpath = "./data/webresearch/"
        files = list.files(dirpath, pattern='blkparse')
        for ( f in files ) {
            f = paste(dirpath, f, sep='')
            d <- load_file_from_cache(f, 'load')
            d = clean(d)

            d = d[10000:20000, ]
            globaltimeend = tail(d$timestamp, 1)
            d = ddply(d, .(blocknum), ddply_timeextent, globaltimeend)
            d$reincarnation = factor(d$reincarnation)
            d$reincarnation = factor(d$reincarnation, levels=sample(levels(d$reincarnation)))
            plot_life(d)

            a = readline()
            if ( a == 'a')
                break
        }
    }
 

    # do_main()
    # do_main2()
    # do_main3()
    do_main_vis_lives()
}

# This function is for the case that when plotting with ggplot,
# some bar is missing so other bars at the same location become
# bigger.
# See evernote title: "fill in missing values" for more details
# id_cols, val_col are characters
set_missing_to_default <- function(d, id_cols, val_col, default_val)
{
    level_list = apply(d[, id_cols], 2, unique)
    d.temp = expand.grid(level_list)
    d.new = merge(d, d.temp, all=T)
    d.new[, val_col] = ifelse(is.na(d.new[, val_col]), default_val, 
                              d.new[, val_col])
    return(d.new)
}


explore.sim.results <- function()
{
    # This function plot all .stats files in a directory
    explore.stats <- function(expdir)
    {
        load <- function(dir)
        {
            files = list.files(dir, recursive = T, 
               pattern = "stats", full.names = T)
            ret = data.frame()
            for (f in files) {
                print(f)
                d = read.csv(f, header=T, sep=';')
                d = melt(d)
                d$file = paste(tail(unlist(strsplit(f, "/")), 2), collapse="/")
                ret = rbind(ret, d)
            }
            print(ret)
            return(ret)
        }

        clean <- function(d)
        {
            return(d)
        }

        func <- function(d)
        {
            # p = ggplot(d, aes(x=variable, y=value, fill=file)) +
            d = set_missing_to_default(d, 
                id_cols=c("file", "variable"), val_col="value",
                default_val = NA)
            p = ggplot(d, aes(x=file, y=value, fill=variable)) +
                geom_bar(stat='identity', position='dodge') + 
                theme(axis.text.x = element_text(angle=90))
            ggsave("tpcc-time-space.png", plot=p, w=50, h=10)
            return()
            return(p)
        }

        do_main <- function(dir)
        {
            d = load(dir)
            d = clean(d)
            func(d)
        }
        # do_main("~/datahouse/seq_randstart/")
        # do_main("~/datahouse/randomlba/")
        # do_main("~/datahouse/seqlba")
        # do_main("~/datahouse/seqlba_improved_dm_pm")
        # do_main("~/datahouse/seqlba_improved_dm_pm_SEQ")
        do_main(expdir)
    }

    explore.trace <- function(expdir)
    {
        transfer <- function()
        {
        }

        load <- function(fpath)
        {
            print(fpath)
            d = read.table(fpath, header=F, col.names = c('type', 'operation', 'pagenum', 'cat'))
            return(d)
        }

        clean <- function(d)
        {
            return(d)
        }

        func <- function(d)
        {
            # print((d))
            # d = head(d, n=100000)
            d$seqid = seq_along(d$operation)

            print(levels(d$operation))
            d$operation = factor(d$operation, levels=c('lba_write', 'page_read', 'page_write', 'lba_discard', 'block_erase'))
            # d = subset(d, operation != 'block_erase')
            # d = subset(d, cat == 'amplified')


            # quartz()
            # d = subset(d, cat == 'user')
            p = ggplot(d, aes(x=seqid, y=pagenum, color=cat)) +
                geom_point() +
                facet_grid(operation~cat) +
                scale_colour_manual(
                  values = c("amplified" = "red",
                             "user" = "blue"))
            # print(p)
            return(p)
            # z = grid.locator()
            # ggsave("plot.pdf", plot=p, h=12, w=4)
        }

        do_main <- function(expdir)
        {
            plotlist = list()
            files = list.files(expdir, recursive = T, 
                pattern = "ftlsim.out$", full.names = T)
            print(files)

            for (f in files ) {
                # d = load_file_from_cache(f, 'load')
                d = load(f)
                d = clean(d)
                p = func(d)

                filename = paste(tail(unlist(strsplit(f, "/")), 4), collapse="/")
                p = p + ggtitle(filename)

                plotlist = append(plotlist, list(p))
            }
            return(plotlist)
        }
        do_main(expdir)
    }

    local_main <- function(expdir) 
    {
        plotlist = explore.trace(expdir)
        p = explore.stats(expdir)
        plotlist = append(plotlist, list(p))
        do.call('grid.arrange', c(plotlist, ncol=1)) 
    }

    # local_main("~/datahouse/long-mdtest/")
    # local_main("~/datahouse/ext4-hybridmap-4096/")
    local_main("~/datahouse/ext4-hybridmap-512/")
}

explore.mywl <- function()
{
    transfer <- function()
    {
    }
    load <- function()
    {
        d = read.csv('./data/finaltable.txt', header=T, sep=';')
        print(head(d))
        return(d)
    }

    clean <- function(d)
    {
        return(d)
    }

    func <- function(d)
    {
        print(summary(d))
        p = ggplot(d, aes(x=time, y=blockstart)) +
            geom_point()
        print(p)
    }

    do_main <- function()
    {
        d = load()
        d = clean(d)
        func(d)
    }
    do_main()
}

explore.function.hist <- function()
{
    transfer <- function()
    {
    }
    load <- function()
    {
        branch5item10.text = "f2fs_writepages:;f2fs_balance_fs;f2fs_statfs;f2fs_write_data_page;f2fs_getattr;f2fs_write_node_page;f2fs_wait_on_page_writeback;f2fs_getxattr;f2fs_lookup;f2fs_find_entry;f2fs_set_meta_page_dirty;f2fs_submit_page_mbio:;f2fs_alloc_inode;f2fs_get_acl;f2fs_init_acl;f2fs_mkdir;f2fs_issue_discard:;f2fs_new_inode;f2fs_set_node_page_dirty;f2fs_dentry_hash;f2fs_dirty_inode;__f2fs_writepage;f2fs_write_checkpoint:;f2fs_submit_write_bio:;f2fs_new_inode:;f2fs_submit_merged_bio;f2fs_submit_page_mbio;f2fs_set_page_dirty:;f2fs_write_end_io;f2fs_reserve_block;f2fs_write_data_pages;tracing_mark_write:;f2fs_issue_discard.isra.12;f2fs_init_security;f2fs_reserve_new_block:;f2fs_writepage:;__f2fs_add_link;f2fs_set_data_page_dirty;f2fs_gc;f2fs_write_meta_page
46;24;1;46;1;46;466;5;24;24;71;163;24;5;23;24;23;24;254;48;116;46;69;93;24;162;163;370;93;47;46;1;23;23;23;163;24;46;23;71"
        branch5item10.d = read.csv(text=branch5item10.text, header=T, sep=';')
        branch5item10.d = melt(branch5item10.d)
        branch5item10.d$run = 'branch5item10'

        branch10item50.text = "f2fs_iget;f2fs_writepages:;f2fs_balance_fs;f2fs_statfs;f2fs_write_data_page;f2fs_getattr;f2fs_write_node_page;f2fs_wait_on_page_writeback;f2fs_getxattr;f2fs_lookup;f2fs_find_entry;f2fs_set_meta_page_dirty;f2fs_submit_page_mbio:;f2fs_alloc_inode;f2fs_get_acl;f2fs_init_acl;f2fs_mkdir;f2fs_issue_discard:;f2fs_new_inode;f2fs_set_node_page_dirty;f2fs_dentry_hash;f2fs_create;f2fs_dirty_inode;__f2fs_writepage;f2fs_write_checkpoint:;f2fs_submit_write_bio:;f2fs_get_victim:;f2fs_iget:;f2fs_new_inode:;f2fs_submit_merged_bio;f2fs_submit_page_mbio;f2fs_set_page_dirty:;f2fs_write_inode;f2fs_write_end_io;f2fs_reserve_block;f2fs_write_data_pages;tracing_mark_write:;f2fs_issue_discard.isra.12;f2fs_init_security;f2fs_reserve_new_block:;f2fs_writepage:;__f2fs_add_link;f2fs_set_data_page_dirty;f2fs_gc;f2fs_write_meta_page
3984;114;69;1;113;1;15665;50129;4;68;68;245;25377;67;4;67;48;55;67;24539;135;20;297;113;178;1082;22;3992;67;437;25377;24914;1;1081;120;114;1;55;67;50;16002;67;114;67;230"
        branch10item50.d = read.csv(text=branch10item50.text, header=T, sep=';')
        branch10item50.d = melt(branch10item50.d)
        branch10item50.d$run = 'branch10item50'

        branch10item50.256mb.text = "f2fs_balance_fs;f2fs_statfs;f2fs_getattr;f2fs_wait_on_page_writeback;f2fs_getxattr;f2fs_lookup;f2fs_find_entry;f2fs_alloc_inode;f2fs_get_acl;f2fs_init_acl;f2fs_mkdir;f2fs_new_inode;f2fs_set_node_page_dirty;f2fs_dentry_hash;f2fs_dirty_inode;f2fs_new_inode:;f2fs_set_page_dirty:;f2fs_reserve_block;tracing_mark_write:;f2fs_init_security;f2fs_reserve_new_block:;__f2fs_add_link;f2fs_set_data_page_dirty
82;1;1;658;5;83;82;81;5;82;82;82;576;163;411;81;741;164;1;82;82;81;165"
        branch10item50.256mb.d = read.csv(text=branch10item50.256mb.text, header=T, sep=';')
        branch10item50.256mb.d = melt(branch10item50.256mb.d)
        branch10item50.256mb.d$run = 'branch10item50.256mb'

        d = rbind(branch5item10.d, branch10item50.d, branch10item50.256mb.d)
        return(d)
    }

    clean <- function(d)
    {
        return(d)
    }

    func <- function(d)
    {
        dd = ddply(d, .(variable), subset, value == max(value))
        vars = as.character(dd$variable)
        vars = vars[order(dd$value)]
        sorted_levels = unique(vars)


        d.temp = expand.grid(variable=unique(sorted_levels), run=unique(d$run))
        d.temp$value_default = 0
        print(nrow(d))
        print(nrow(d.temp))

        print(head(d))
        print(d.temp)
        d = merge(d, d.temp, all=T)
        d$final_value = apply(d[, c('value', 'value_default')], 1, max)

        d$variable = factor(d$variable, levels=sorted_levels)
        print(d)
        print(subset(d, variable == 'X__f2fs_writepage'))
        p = ggplot(d, aes(x=variable, y=final_value, fill=run), dropping=F) +
            geom_bar(stat='identity', position='dodge') +
            coord_flip()
        print(p)
    }

    do_main <- function()
    {
        d = load()
        d = clean(d)
        func(d)
    }
    do_main()
}

main <- function()
{
    # explore.FSJ386323()
    # explore.FIU()
    # explore.madmax.1.blkparse()
    # explore.madmax.iterate()
    # explore.mail01()
    # explore.websearch()
    explore.sim.results()
    # explore.mywl()
    # explore.stats()
    # explore.function.hist()
}
main()
