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

explore.sim.results <- function()
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

    do_main <- function()
    {
        plotlist = list()
        files = c( 
            # '~/datahouse/randomlba/directmap/ftlsim.out',
            # '~/datahouse/randomlba/blockmap/ftlsim.out',
            # '~/datahouse/randomlba/pagemap/ftlsim.out',
            # '~/datahouse/randomlba/hybridmap/ftlsim.out'

            # '~/datahouse/seqlba/directmap/ftlsim.out',
            # '~/datahouse/seqlba/blockmap/ftlsim.out',
            # '~/datahouse/seqlba/pagemap/ftlsim.out',
            # '~/datahouse/seqlba/hybridmap/ftlsim.out'

            # '~/datahouse/seq_randstart/directmap/ftlsim.out',
            # '~/datahouse/seq_randstart/blockmap/ftlsim.out',
            # '~/datahouse/seq_randstart/pagemap/ftlsim.out',
            # '~/datahouse/seq_randstart/hybridmap/ftlsim.out'

            # '~/datahouse/randomlba/pagemap/ftlsim.out'
            # '~/datahouse/seqlba_03/pagemap/ftlsim.out'

            # '~/datahouse/seqlba_improved_dm_pm/directmap/ftlsim.out',
            # '~/datahouse/seqlba_improved_dm_pm/blockmap/ftlsim.out',
            # '~/datahouse/seqlba_improved_dm_pm/pagemap/ftlsim.out',
            # '~/datahouse/seqlba_improved_dm_pm/hybridmap/ftlsim.out'

            '~/datahouse/seqlba_improved_dm_pm_SEQ/directmap/ftlsim.out',
            '~/datahouse/seqlba_improved_dm_pm_SEQ/blockmap/ftlsim.out',
            '~/datahouse/seqlba_improved_dm_pm_SEQ/pagemap/ftlsim.out',
            '~/datahouse/seqlba_improved_dm_pm_SEQ/hybridmap/ftlsim.out'
            )

        # dir = "~/datahouse/mdtest"
        # dir = "~/datahouse/mdtest-btrfs/"
        dir = "~/datahouse/mdtest-3fs/"
        files = list.files(dir, recursive = T, 
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

            # a = readline()
            a = 0
            if (a == 'a') {
                break
            }
        }
        do.call('grid.arrange', c(plotlist, ncol=1)) 
    }
    do_main()
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

# This function plot all .stats files in a directory
explore.stats <- function()
{
    transfer <- function()
    {
    }
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
        p = ggplot(d, aes(x=file, y=value, fill=variable)) +
            geom_bar(stat='identity', position='dodge') + 
            theme(axis.text.x = element_text(angle=90))
        print(p)
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
    do_main("~/datahouse/mdtest/")
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
}
main()
