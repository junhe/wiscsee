# libraries
library(ggplot2)
library(plyr)
library(reshape2)
library(gridExtra)
library(jsonlite)
library(digest)

# copy the following so you can do sme()
WORKDIRECTORY= "/Users/junhe/BoxSync/0-MyResearch/Doraemon/workdir/doraemon/analysis"
THISFILE     ='analyzer.r'
setwd(WORKDIRECTORY)
sme <- function()
{
    setwd(WORKDIRECTORY)
    source(THISFILE)
}

cbPalette <- c("#89C5DA", "#DA5724", "#74D944", "#CE50CA", "#3F4921", "#C0717C", "#CBD588", "#5F7FC7", 
    "#673770", "#D3D93E", "#38333E", "#508578", "#D7C1B1", "#689030", "#AD6F3B", "#CD9BCD", 
    "#D14285", "#6DDE88", "#652926", "#7FDCC0", "#C84248", "#8569D5", "#5E738F", "#D1A33D", 
    "#8A7C64", "#599861")

split_column <- function(mycol, splitchar)
{
    mycol = as.character(mycol)
    splitlist = strsplit(mycol, splitchar, fixed=T)
    df = ldply(splitlist, as.vector)
    return(df)
}

split_column_to_columns <- function( char_vec, target_ncols, splitchar )
{
    fill_na <- function(items)
    {
        n = length(items)
        delta = target_ncols - n
        if (delta > 0) {
            items = append(items, rep(NA, delta))
        }
        if (delta < 0) {
            stop("target_ncols must be larger than length(items)")
        }
        return(items)
    }
 
    l = strsplit(char_vec, splitchar, fixed=T)
    ret = do.call('rbind', lapply(l, fill_na))
    ret = as.data.frame(ret)
    return(ret)
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

rename_cat <- function(cate)
{
    cate = revalue(cate, c("user"       = "forground",
                           "amplified"  = "background"))
    return(cate)
}

rename_combined_label <- function(labels)
{
    # e.g. lba_write -> logical_write
    #      amplified -> background
    dd = split_column(labels, '.')
    dd[,1] = rename_operation(dd[,1])
    dd[,2] = rename_cat(dd[,2])
    return(paste(dd[,1], dd[,2], sep='.'))
}

get_config <- function(jsonpath)
{
    doc <- fromJSON(txt=jsonpath)
    return(doc)
}

datafile_to_conffile <- function(path)
{
    elems = unlist(strsplit(path, '/'))
    lastindex = length(elems)
    if ( lastindex == 0 ) {
        stop('lastindex cannot be 0')
    }
    elems[lastindex] = 'config.json'
    return(paste(elems, collapse='/'))
}

get_conf_by_datafile_path <- function(datafilepath)
{
    confpath = datafile_to_conffile(datafilepath)
    return( get_config(confpath) )
}

get_fs_with_hash <-function(f)
{
    conf = get_conf_by_datafile_path(f)
    hash = substr(digest(list(conf, f), algo="md5", serialize=T), 0, 4)
    return(paste(conf[['filesystem']], hash, sep='.'))
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
        # print('filecache not exist')
        filecache <<- list()
    } else {
        # print('filecache exist')
    }
    
    if ( exists(fpath, where=filecache) == TRUE ) {
        # print('in cache')
        return(filecache[[fpath]])
    } else {
        # not in cache
        # print('not in cache')
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
    level_list = lapply(as.list(d[, id_cols]), unique)
    d.temp = expand.grid(level_list)
    d.new = merge(d, d.temp, all=T)
    d.new[, val_col] = ifelse(is.na(d.new[, val_col]), default_val, 
                              d.new[, val_col])
    return(d.new)
}

explore.sim.results.for.meeting.0702 <- function()
{
    get_config <- function(jsonpath)
    {
        doc <- fromJSON(txt=jsonpath)
        return(doc)
    }

    datafile_to_conffile <- function(path)
    {
        elems = unlist(strsplit(path, '/'))
        lastindex = length(elems)
        if ( lastindex == 0 ) {
            stop('lastindex cannot be 0')
        }
        elems[lastindex] = 'config.json'
        return(paste(elems, collapse='/'))
    }

    split_column <- function(mycol, splitchar)
    {
        mycol = as.character(mycol)
        splitlist = strsplit(mycol, splitchar, fixed=T)
        df = ldply(splitlist, as.vector)
        return(df)
    }

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

                confpath = datafile_to_conffile(f)
                conf = get_config(confpath)
                d$bench.to.run = conf[['sqlbench']][['benches_to_run']]
                d$filesystem = conf[['filesystem']]

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
            d2 = set_missing_to_default(d, 
                id_cols=c("file", "variable"), val_col="value",
                default_val = NA)
            d2$bench.to.run = NULL

            d1 = ddply(d, .(file), head, 1)
            d1 = d1[, c('file', 'bench.to.run')]
            d = merge(d1, d2, by = c('file'))

            l = strsplit(d$file, '/')
            l = lapply(l, '[[', 1)
            l = lapply(l, strsplit, '-')
            l = lapply(l, '[[', 1)
            l = lapply(l, '[', c(1,2))
            l = lapply(l, paste, collapse='-')
            l = unlist(l)
            d$fs = l
            d = subset(d, fs != 'ext4-hybridmap')
            d$file = split_column(d$fs, '-')[,1]
            d$file = factor(d$file, levels=c('btrfs', 'f2fs', 'ext4'))

            p = ggplot(d, aes(x=file, y=value, fill=variable)) +
                geom_bar(stat='identity', position='dodge') + 
                facet_grid(~bench.to.run, scale="free_x") +
                theme(axis.text.x = element_text(angle=90)) +
                xlab("file system") +
                ylab("count")
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

        func <- function(d, datafile=NULL)
        {
            d$seqid = seq_along(d$operation)

            d$operation = factor(d$operation, levels=c('lba_write', 
                'page_read', 'page_write', 'lba_discard', 'block_erase'))

            confpath = datafile_to_conffile(datafile)
            conf = get_config(confpath)
            flash_npage_per_block = conf[['flash_npage_per_block']]
            d$pagenum[d$operation == 'block_erase'] = d$pagenum[d$operation == 'block_erase'] *
                flash_npage_per_block

            p = ggplot(d, aes(x=seqid, y=pagenum, color=cat)) +
                geom_point() +
                facet_grid(operation~cat) +
                scale_colour_manual(
                  values = c("amplified" = "red",
                             "user" = "blue"))
            return(p)
        }

        do_main <- function(expdir)
        {
            plotlist = list()
            files = list.files(expdir, recursive = T, 
                pattern = "ftlsim.out$", full.names = T)

            for (f in files ) {
                d = load_file_from_cache(f, 'load')
                # d = load(f)
                d = clean(d)
                p = func(d, datafile=f)

                filename = paste(tail(unlist(strsplit(f, "/")), 4), collapse="/")
                p = p + ggtitle(filename)

                plotlist = append(plotlist, list(p))

                break
            }
            print(df.lba)
            return(plotlist)
        }
        do_main(expdir)
    }

    local_main <- function(expdir) 
    {
        plotlist = list()
        # plotlist = append(plotlist, explore.trace(expdir))
        p = explore.stats(expdir)
        plotlist = append(plotlist, list(p))
        do.call('grid.arrange', c(plotlist, ncol=1)) 
    }

    # local_main("~/datahouse/long-mdtest/")
    # local_main("~/datahouse/ext4-hybridmap-4096/")
    # local_main("~/datahouse/ext4-hybridmap-512/")
    local_main("~/datahouse/sqlbench-1by1")
}

explore.sim.results <- function()
{
    get_config <- function(jsonpath)
    {
        doc <- fromJSON(txt=jsonpath)
        return(doc)
    }

    datafile_to_conffile <- function(path)
    {
        elems = unlist(strsplit(path, '/'))
        lastindex = length(elems)
        if ( lastindex == 0 ) {
            stop('lastindex cannot be 0')
        }
        elems[lastindex] = 'config.json'
        return(paste(elems, collapse='/'))
    }

    split_column <- function(mycol, splitchar)
    {
        mycol = as.character(mycol)
        splitlist = strsplit(mycol, splitchar, fixed=T)
        df = ldply(splitlist, as.vector)
        return(df)
    }

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
                theme(axis.text.x = element_text(angle=90)) +
                coord_flip()
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
        explore_page_access_freq <- function(d)
        {
            cnt = as.data.frame(table(d$pagenum))
            print(head(cnt))
            freqdist = as.data.frame(table(cnt$Freq))
            print(summary(freqdist))
            freqdist$Var1 = as.numeric(freqdist$Var1)

            p = ggplot(freqdist, aes(x=Var1, y=Freq)) +
                geom_point() +
                geom_line() +
                scale_y_log10() +
                xlab('x is access count, y is number of blocks having this count') +
                ylab('block count (log10)')
            return(p)
        }

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

        func <- function(d, datafile=NULL)
        {
            d$seqid = seq_along(d$operation)

            d$operation = factor(d$operation, levels=c('lba_write', 
                'page_read', 'page_write', 'lba_discard', 'block_erase'))

            confpath = datafile_to_conffile(datafile)
            conf = get_config(confpath)
            flash_npage_per_block = conf[['flash_npage_per_block']]
            d$pagenum[d$operation == 'block_erase'] = d$pagenum[d$operation == 'block_erase'] *
                flash_npage_per_block

            p = ggplot(d, aes(x=seqid, y=pagenum, color=cat)) +
                geom_point() +
                facet_grid(operation~cat) +
                scale_colour_manual(
                  values = c("amplified" = "red",
                             "user" = "blue"))
            return(p)
        }

        do_main <- function(expdir)
        {
            plotlist = list()
            files = list.files(expdir, recursive = T, 
                pattern = "ftlsim.out$", full.names = T)
            print(files)

            # order so the same bench goes together
            l = strsplit(files, '-')
            l = lapply(l, tail, 1)
            l = unlist(l)
            files = files[order(l)]
            print(files)

            for (f in files ) {
                d = load_file_from_cache(f, 'load')
                # d = load(f)
                d = clean(d)

                filename = paste(tail(unlist(strsplit(f, "/")), 4), collapse="/")

                p = func(d, datafile=f)
                p = p + ggtitle(filename)

                print(head(d))
                p.freq = explore_page_access_freq(subset(d, operation == 'lba_write'))
                p.freq = p.freq + ggtitle(filename)

                plotlist = append(plotlist, list(p, p.freq))
            }
            return(plotlist)
        }
        do_main(expdir)
    }

    local_main <- function(expdir) 
    {
        plotlist = list()
        plotlist = explore.trace(expdir)
        p = explore.stats(expdir)
        plotlist = append(plotlist, list(p))
        do.call('grid.arrange', c(plotlist, ncol=1)) 
    }

    # local_main("~/datahouse/long-mdtest/")
    # local_main("~/datahouse/ext4-hybridmap-4096/")
    # local_main("~/datahouse/ext4-hybridmap-512/")
    # local_main("~/datahouse/sqlbench-1by1")
    local_main("~/datahouse/sequential6")
}

explore.sim.results.too.new <- function()
{
    get_config <- function(jsonpath)
    {
        doc <- fromJSON(txt=jsonpath)
        return(doc)
    }

    datafile_to_conffile <- function(path)
    {
        elems = unlist(strsplit(path, '/'))
        lastindex = length(elems)
        if ( lastindex == 0 ) {
            stop('lastindex cannot be 0')
        }
        elems[lastindex] = 'config.json'
        return(paste(elems, collapse='/'))
    }

    split_column <- function(mycol, splitchar)
    {
        mycol = as.character(mycol)
        splitlist = strsplit(mycol, splitchar, fixed=T)
        df = ldply(splitlist, as.vector)
        return(df)
    }

    # This function plot all .stats files in a directory
    explore.stats <- function(expdir)
    {
        load <- function(dir)
        {
            files = list.files(dir, recursive = T, 
               pattern = "ftlsim.out.stats", full.names = T)
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
                scale_fill_manual(values=cbPalette) +
                theme(axis.text.x = element_text(angle=90)) +
                coord_flip()
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
        explore_page_access_freq <- function(d)
        {
            cnt = as.data.frame(table(d$pagenum))
            freqdist = as.data.frame(table(cnt$Freq))
            freqdist$Var1 = as.numeric(freqdist$Var1)

            p = ggplot(freqdist, aes(x=Var1, y=Freq)) +
                geom_point() +
                geom_line() +
                scale_y_log10() +
                expand_limits(y=0.01) +
                xlab('x is access count, y is number of blocks having this count') +
                ylab('block count (log10)')
            return(p)
        }

        transfer <- function()
        {
        }

        load <- function(fpath)
        {
            d = read.table(fpath, header=F, col.names = c('type', 'operation', 'pagenum', 'cat'))
            return(d)
        }

        clean <- function(d)
        {
            return(d)
        }

        func <- function(d, datafile=NULL)
        {
            d$seqid = seq_along(d$operation)

            print(summary(d$operation))
            d$operation = factor(d$operation, levels=c('lba_read', 'lba_write', 
                'page_read', 'page_write', 'lba_discard', 'block_erase'))

            confpath = datafile_to_conffile(datafile)
            conf = get_config(confpath)
            flash_npage_per_block = conf[['flash_npage_per_block']]
            d$pagenum[d$operation == 'block_erase'] = d$pagenum[d$operation == 'block_erase'] *
                flash_npage_per_block

            p = ggplot(d, aes(x=seqid, y=pagenum, color=cat)) +
                geom_point() +
                facet_grid(operation~cat) +
                scale_colour_manual(
                  values = c("amplified" = "red",
                             "user" = "blue"))
            return(p)
        }

        get_unique_lba_row <- function(d, datafile)
        {
            unique.lba.count = length(unique(subset(d, operation == 'lba_write')$pagenum)) 
            return(c('lba.cnt'=unique.lba.count, 'datafile'=datafile))
        }

        do_main <- function(expdir)
        {
            plotlist = list()
            files = list.files(expdir, recursive = T, 
                pattern = "ftlsim.out$", full.names = T)
            df.lba = list()

            # order so the same bench goes together
            l = strsplit(files, '-')
            l = lapply(l, tail, 1)
            l = unlist(l)
            files = files[order(l)]

            for (f in files ) {
                print(f)
                d = load_file_from_cache(f, 'load')
                # d = load(f)
                d = clean(d)

                filename = paste(tail(unlist(strsplit(f, "/")), 4), collapse="/")

                p = func(d, datafile=f)
                p = p + ggtitle(filename)

                p.freq = explore_page_access_freq(subset(d, operation == 'lba_write'))
                p.freq = p.freq + ggtitle(filename)

                plotlist = append(plotlist, list(p, p.freq))

                df.lba = append(df.lba, list(get_unique_lba_row(d, filename)))
            }
            df.lba = as.data.frame(matrix(unlist(df.lba), ncol=2, byrow=T))
            names(df.lba) = c("lba.cnt", "datafile")
            df.lba$lba.cnt = as.numeric(as.character(df.lba$lba.cnt))

            p = ggplot(df.lba, aes(x=datafile, y=lba.cnt))+
                geom_bar(stat='identity', position='dodge') +
                theme(axis.text.x = element_text(angle=90)) +
                ylab('unique.lba.count') +
                coord_flip()

            plotlist = append(plotlist, list(p))

            return(plotlist)
        }
        do_main(expdir)
    }

    local_main <- function(expdir) 
    {
        plotlist = list()
        plotlist = explore.trace(expdir)
        p = explore.stats(expdir)
        plotlist = append(plotlist, list(p))
        do.call('grid.arrange', c(plotlist, ncol=1)) 
    }

    # local_main("~/datahouse/long-mdtest/")
    # local_main("~/datahouse/ext4-hybridmap-4096/")
    # local_main("~/datahouse/ext4-hybridmap-512/")
    # local_main("~/datahouse/sequential10")
    # local_main("~/datahouse/sequential.btrfs")
    # local_main("~/datahouse/sequential.btrfs.nocow")
    # local_main("~/datahouse/sequential14")
    # local_main("~/datahouse/sequential.discard")
    # local_main("~/datahouse/backwards")
    # local_main("~/datahouse/withfsync")
    # local_main("~/datahouse/forward.blktrace.right")
    local_main("~/datahouse/backwards.blktrace.right")
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


explore.sim.results.alter.table.for.meeting.0702 <- function()
{
    get_config <- function(jsonpath)
    {
        doc <- fromJSON(txt=jsonpath)
        return(doc)
    }

    datafile_to_conffile <- function(path)
    {
        elems = unlist(strsplit(path, '/'))
        lastindex = length(elems)
        if ( lastindex == 0 ) {
            stop('lastindex cannot be 0')
        }
        elems[lastindex] = 'config.json'
        return(paste(elems, collapse='/'))
    }

    split_column <- function(mycol, splitchar)
    {
        mycol = as.character(mycol)
        splitlist = strsplit(mycol, splitchar, fixed=T)
        df = ldply(splitlist, as.vector)
        return(df)
    }

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

                confpath = datafile_to_conffile(f)
                conf = get_config(confpath)
                d$bench.to.run = conf[['sqlbench']][['benches_to_run']]
                d$filesystem = conf[['filesystem']]

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
            d2 = set_missing_to_default(d, 
                id_cols=c("file", "variable"), val_col="value",
                default_val = NA)
            d2$bench.to.run = NULL

            d1 = ddply(d, .(file), head, 1)
            d1 = d1[, c('file', 'bench.to.run')]
            d = merge(d1, d2, by = c('file'))

            l = strsplit(d$file, '/')
            l = lapply(l, '[[', 1)
            l = lapply(l, strsplit, '-')
            l = lapply(l, '[[', 1)
            l = lapply(l, '[', c(1,2))
            l = lapply(l, paste, collapse='-')
            l = unlist(l)
            d$fs = l
            d = subset(d, fs != 'ext4-hybridmap')
            d$file = split_column(d$fs, '-')[,1]
            d$file = factor(d$file, levels=c('btrfs', 'f2fs', 'ext4'))

            p = ggplot(d, aes(x=file, y=value, fill=variable)) +
                geom_bar(stat='identity', position='dodge') + 
                facet_grid(~bench.to.run, scale="free_x") +
                theme(axis.text.x = element_text(angle=90)) +
                xlab("file system") +
                ylab("count")
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
        explore_page_access_freq <- function(d)
        {
            cnt = as.data.frame(table(d$pagenum))
            print(head(cnt))
            freqdist = as.data.frame(table(cnt$Freq))
            print(summary(freqdist))
            freqdist$Var1 = as.numeric(freqdist$Var1)

            p = ggplot(freqdist, aes(x=Var1, y=Freq)) +
                geom_point() +
                geom_line() +
                scale_y_log10(limits = c(0.01, 10^7)) +
                xlab('x is access count, y is number of blocks having this count') +
                ylab('block count')
            return(p)
        }

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

        func <- function(d, datafile=NULL)
        {
            d$seqid = seq_along(d$operation)

            d$operation = factor(d$operation, levels=c('lba_write', 
                'page_read', 'page_write', 'lba_discard', 'block_erase'))

            confpath = datafile_to_conffile(datafile)
            conf = get_config(confpath)
            flash_npage_per_block = conf[['flash_npage_per_block']]
            d$pagenum[d$operation == 'block_erase'] = d$pagenum[d$operation == 'block_erase'] *
                flash_npage_per_block

            p = ggplot(d, aes(x=seqid, y=pagenum, color=cat)) +
                geom_point() +
                facet_grid(operation~cat) +
                scale_colour_manual(
                  values = c("amplified" = "red",
                             "user" = "blue"))
            return(p)
        }

        do_main <- function(expdir)
        {
            plotlist = list()
            files = list.files(expdir, recursive = T, 
                pattern = "ftlsim.out$", full.names = T)
            print(files)

            # order so the same bench goes together
            l = strsplit(files, '-')
            l = lapply(l, tail, 1)
            l = unlist(l)
            files = files[order(l)]
            print(files)

            for (f in files ) {
                if ( ! grepl('alter-table', f) ) {
                    next
                }

                d = load_file_from_cache(f, 'load')
                # d = load(f)
                d = clean(d)

                filename = paste(tail(unlist(strsplit(f, "/")), 4), collapse="/")

                cleanfilename = gsub('/', '-', f)
                p = func(d, datafile=f)
                p = p + ggtitle(filename)
                # ggsave(paste(cleanfilename, 'trace.pdf', sep='-'), plot=p, w=10, h=20)
                ggsave(paste(cleanfilename, 'trace.png', sep='-'), plot=p, w=10, h=20)

                print(head(d))
                p.freq = explore_page_access_freq(subset(d, operation == 'lba_write'))
                p.freq = p.freq + ggtitle(filename)
                ggsave(paste(cleanfilename, 'count.pdf', sep='-'), plot=p.freq, w=12, h=8)

                plotlist = append(plotlist, list(p, p.freq))
            }
            return(plotlist)
        }
        do_main(expdir)
    }

    local_main <- function(expdir) 
    {
        plotlist = list()
        plotlist = explore.trace(expdir)
        # p = explore.stats(expdir)
        # plotlist = append(plotlist, list(p))
        # do.call('grid.arrange', c(plotlist, ncol=1)) 
    }

    # local_main("~/datahouse/long-mdtest/")
    # local_main("~/datahouse/ext4-hybridmap-4096/")
    # local_main("~/datahouse/ext4-hybridmap-512/")
    local_main("~/datahouse/sqlbench-1by1")
}


explore.stack <- function()
{
    # this is a refactoring of the ole explore.sim.result. 
    # subfunction should take data frame as argument, in that case
    # you don't need to read file every time you call the function
    # So the process is like
    # analyze.df (data.frame) {}
    # analyze.df2 (data.frame) {}
    # load (file.path)
    # analyze.file(filepath){
    #     d = load(filepath)
    #     analyze(d)
    #     analyze2(d)
    # }
    # analyze.dir(dir.path) {
    #     for f in file:
    #         analyze.file(f)
    #     plot
    # }

    get_config <- function(jsonpath)
    {
        doc <- fromJSON(txt=jsonpath)
        return(doc)
    }

    datafile_to_conffile <- function(path)
    {
        elems = unlist(strsplit(path, '/'))
        lastindex = length(elems)
        if ( lastindex == 0 ) {
            stop('lastindex cannot be 0')
        }
        elems[lastindex] = 'config.json'
        return(paste(elems, collapse='/'))
    }

    get_conf_by_datafile_path <- function(datafilepath)
    {
        confpath = datafile_to_conffile(datafilepath)
        return( get_config(confpath) )
    }

    get_fs_with_hash <-function(f)
    {
        conf = get_conf_by_datafile_path(f)
        hash = substr(digest(list(conf, f), algo="md5", serialize=T), 0, 4)
        return(paste(conf[['filesystem']], hash, sep='.'))
    }

    analyze.dir.ftilsim.out <- function(dir.path)
    {
        func.sequentiality <- function(d, datafile)
        {
            # this function returns the number of lba pages (x) that are
            # accessed right after page (x-1)
            d = subset(d, operation == 'lba_write')
            addr.pre.pagenum = d$pagenum - 1
            size = length(d$pagenum)
            is.seq = d$pagenum[-size] == addr.pre.pagenum[-1]
            
            diff = d$pagenum[-size] - addr.pre.pagenum[-1]
            diff = diff[ diff != 0 ]

            p = qplot(seq_along(diff), diff) +
                ggtitle(datafile)
            print(p)

            # return (list('sequential writes:'=length(which(is.seq)), 
                         # 'total write'=length(is.seq)))
        }

        func.page.access.freq <- function(d, datafile)
        {
            cnt = as.data.frame(table(d$pagenum))
            freqdist = as.data.frame(table(cnt$Freq))
            freqdist$Var1 = as.numeric(freqdist$Var1)

            p = ggplot(freqdist, aes(x=Var1, y=Freq)) +
                geom_point() +
                geom_line() +
                scale_y_log10() +
                expand_limits(y=0.01) +
                xlab('x is access count, y is number of blocks having this count') +
                ylab('block count (log10)') +
                ggtitle(datafile)
            print(p)
        }

        func.space.time <- function(d, datafile=NULL)
        {

            print(summary(d$operation))
            print(head(d))
            d$operation = factor(d$operation, levels=c(
                'logical_discard', 'logical_write', 'logical_read', 'phy_block_erase', 
                'physical_read', 'physical_write'))
                                                       # 'lba_read', 'lba_write', 
                # 'page_read', 'page_write', 'lba_discard', 'block_erase'))

            confpath = datafile_to_conffile(datafile)
            conf = get_config(confpath)
            flash_npage_per_block = conf[['flash_npage_per_block']]
            d$pagenum[d$operation == 'phy_block_erase'] = d$pagenum[d$operation == 'phy_block_erase'] *
                flash_npage_per_block

            d = subset(d, operation == 'physical_write' & cat %in% c('data.cleaning', 'data.user'))
            d$seqid = seq_along(d$operation)

            d = subset(d, seqid > 6*10^5 - 100 & seqid < 6*10^5 + 100)

            d$blockcolor = (d$pagenum %/% 32) 
            d$blockcolor = factor(d$blockcolor)

            p = ggplot(d, aes(x=seqid, y=pagenum, color=blockcolor)) +
                geom_point() +
                # facet_grid(operation~cat) +
                # scale_colour_manual(
                  # values = c("amplified" = "red",
                             # "user" = "blue")) +
                ggtitle(datafile)
            print(p)
        }

        func.unique.lba.count <- function(d)
        {
            d = subset(d, operation == 'lba_write')  
            d = ddply(d, .(file), 
                function(dd){return(length(unique(dd$pagenum)))})
            d = rename(d, c("V1"='count'))

            p = ggplot(d, aes(x=file, y=count)) +
                geom_bar(stat='identity') +
                ylab("unique.lba.count") +
                coord_flip() 
            print(p)
        }
        
        load.ftlsim.out <- function(fpath)
        {
            d = read.table(fpath, header=F, col.names = c('type', 'operation', 'pagenum', 'cat'))
            d$file = paste(unlist(strsplit(fpath, '/'))[-c(1:4)], collapse='/')
            return(d)
        }
    
        do_main <- function(dir.path) 
        {
            files = list.files(dir.path, recursive = T, 
                pattern = "ftlsim.out$", full.names = T)

            # order so the same bench goes together
            l = strsplit(files, '-')
            l = lapply(l, tail, 1)
            l = unlist(l)
            files = files[order(l)]

            d.all = data.frame()
            for (f in files ) {
                print(f)
                d = load_file_from_cache(f, 'load.ftlsim.out')
                # func.sequentiality(d, f)
                # func.page.access.freq(d, f)
                func.space.time(d, f)
                d.all = rbind(d.all, d)
            }
            # func.unique.lba.count(d.all)
        }
        
        do_main(dir.path)
    }

    analyze.dir.gc.log <- function(dir.paths)
    {
        # Deprecated function to show invalid ratio of GCed blocks
        load.gc.log <- function(fpath)
        {
            # DEBUG victimblock 619 vnv_ratio 1.0
            d = read.table(fpath, header=F,
                colClasses = c("NULL", "NULL", "numeric",      "NULL", "numeric"),
                col.names  = c('',     "",     'victim.block', "",     'inv.ratio')
                )
            return(d)
        }

        load.dir <- function(dir.paths)
        {
            d.all = data.frame()
            for (dir.path in dir.paths) {
                files = list.files(dir.path, recursive = T, 
                    pattern = "gc.log$", full.names = T)

                for (f in files ) {
                    print(f)
                    # d = load_file_from_cache(f, 'load.gc.log')
                    d = load.gc.log(f)
                    if (nrow(d) == 0) {
                        next
                    }
                    conf = get_conf_by_datafile_path(f)
                    d$fs = conf[['filesystem']]
                    d.all = rbind(d.all, d)
                }

            }
            d.all$valid.ratio = 1-d.all$inv.ratio
            return(d.all)
        }

        func <- function(d)
        {
            print(d)
            d$valid.ratio = 1-d$inv.ratio
            p = ggplot(d, aes(x=valid.ratio, fill=fs)) +
                geom_histogram() +
                facet_grid(fs~.)
            print(p)
        }

        do_main <- function(dir.paths) 
        {
            d = load.dir(dir.paths)
            func(d)
        }
        do_main(dir.paths)
    }

    analyze.dir.events.for.ftlsim <- function(dir.path)
    {
        load.events.for.ftlsim <- function(fpath)
        {
            d = read.table(fpath, header=F, col.names = c('operation', 'offset', 'size'))
            return(d)
        }

        func.events.for.ftlsim <- function(d, datafile)
        {
            d$id = seq_along(d$offset)
            p = ggplot(d, aes()) +
                geom_segment(aes(x=id, xend=id, y=offset, yend=offset+size)) +
                ggtitle(datafile)
            print(p)
        }

        who.has.it <- function(d, offset)
        {
            d$end = d$offset + d$size
            d$has.it = d$offset <= offset & d$end > offset
            print(paste('has', offset, '(page:', offset/4096, ')?'))
            print(d[which(d$has.it),])
        }

        files = list.files(dir.path, recursive = T, 
            pattern = "blkparse-events-for-ftlsim.txt$", full.names = T)

        for (f in files ) {
            if (! grepl('btrfs', f)) {
                next
            }
            d = load_file_from_cache(f, 'load.events.for.ftlsim')
            # print(head(d))
            # func.events.for.ftlsim(d, datafile=f)
            who.has.it(d, 1066*4096)
            who.has.it(d, 1088*4096)
        }
    }

    analyze.dir.events.for.ftlsim2 <- function(dir.paths)
    {

        load.events.for.ftlsim <- function(fpath)
        {
            d = read.table(fpath, header=F, col.names = c('operation', 'offset', 'size'))
            return(d)
        }

        load <- function(dir.paths)
        {
            ret = data.frame()
            for (subdir in dir.paths) {
                files = list.files(subdir, recursive = T, 
                   pattern = "blkparse-events-for-ftlsim.txt$", full.names = T)

                for (f in files) {
                    print(f)
                    d = load.events.for.ftlsim(f)
                    d$file = paste(tail(unlist(strsplit(f, "/")), 3), collapse="/")

                    conf = get_conf_by_datafile_path(f)
                    d$filesystem = as.character(conf['filesystem'])
                    d$interface_level = as.character(conf['interface_level'])
                    d$subexpname = as.character(conf['subexpname'])

                    # d$file = f
                    ret = rbind(ret, d)
                }
            }
            return(ret)
        }

        func <- function(d)
        {
            print(head(d))
            dd = as.data.frame(table(d$size/1024, d$filesystem, d$interface_level, d$subexpname))
            print(subset(dd, Var3 == 'range'))
            p = ggplot(d, aes(x=factor(size/1024))) +
                geom_histogram() +
                facet_grid(filesystem~interface_level) +
                theme(axis.text.x = element_text(angle=90)) 
            print(p)
        }

        do_main <- function(dir.paths)
        {
            d = load(dir.paths)
            func(d)
        }

        do_main(dir.paths)
    }

    analyze.dir.stats <- function(dir.paths)
    {
        load.dir.stats <- function(dir.paths)
        {
            ret = data.frame()
            for (subdir in dir.paths) {
                files = list.files(subdir, recursive = T, 
                   pattern = "ftlsim.out.stats", full.names = T)

                for (f in files) {
                    print(f)
                    d = read.csv(f, header=T, sep=';')
                    d = melt(d)
                    d$file = paste(tail(unlist(strsplit(f, "/")), 3), collapse="/")
                    # d$file = f
                    ret = rbind(ret, d)
                }
            }
            return(ret)
 
        }

        func <- function(d)
        {
            # p = ggplot(d, aes(x=variable, y=value, fill=file)) +
            d = set_missing_to_default(d, 
                id_cols=c("file", "variable"), val_col="value",
                default_val = NA)
            p = ggplot(d, aes(x=file, y=value, fill=variable)) +
                geom_bar(stat='identity', position='dodge') + 
                theme(axis.text.x = element_text(angle=90)) +
                coord_flip()
            print(p)
            return(p)
        }

        do_main <- function(dir.paths)
        {
            d = load.dir.stats(dir.paths)
            func(d)
        }

        do_main(dir.paths)
    }

    analyze.dir.gc_cnt.log.put_and_count.stats <- function(dir.paths)
    {
        load.dir.stats <- function(dir.paths)
        {
            ret = data.frame()
            for (subdir in dir.paths) {
                files = list.files(subdir, recursive = T, 
                   pattern = "gc_cnt.log.put_and_count.stats$", full.names = T)

                for (f in files) {
                    print(f)
                    d = read.csv(f, header=T, sep=';')
                    d = melt(d)
                    d$file = paste(tail(unlist(strsplit(f, "/")), 3), collapse="/")
                    # d$file = f
                    ret = rbind(ret, d)
                }
            }
            return(ret)
 
        }

        func <- function(d)
        {
            d = set_missing_to_default(d, 
                id_cols=c("file", "variable"), val_col="value",
                default_val = NA)
            p = ggplot(d, aes(x=file, y=value, fill=variable)) +
                geom_bar(stat='identity', position='dodge') + 
                geom_text(aes(label=value),
                  position=position_dodge(width=1), hjust=1) +
                theme(axis.text.x = element_text(angle=90)) +
                scale_y_log10(breaks=c(0.1, 1, 10, 100, 1000, 10000, 100000, 1000000)) +
                coord_flip()
            print(p)
            return(p)

        }

        do_main <- function(dir.paths)
        {
            d = load.dir.stats(dir.paths)
            func(d)
        }

        do_main(dir.paths)
    }

    analyze.dir.ftlsim.out.stats <- function(expdirs)
    {
        # This function takes a vector of directories as input
        load <- function(expdirs)
        {
            ret = data.frame()
            for (expdir in expdirs) {
                files = list.files(expdir, recursive = T, 
                   pattern = "ftlsim.out.stats$", full.names = T)
                for (f in files) {
                    # if (! grepl('ext4', f)) {
                        # next
                    # }
                    # conf = get_conf_by_datafile_path(f)
                    print(f)
                    d = read.csv(f, header=T, sep=';')
                    d = melt(d)
                    conf = get_conf_by_datafile_path(f)
                    d$filesystem = as.character(conf['filesystem'])
                    d$ftl_type = as.character(conf['ftl_type'])
                    d$subexpname = as.character(conf['subexpname'])
                    # print(head(d))
                    fileitems = unlist(strsplit(f, "/"))
                    d$file = paste(fileitems[-c(1:4, length(fileitems))], collapse="/")
                    ret = rbind(ret, d)
                }
            }
            return(ret)
        }

        clean <- function(d)
        {
            return(d)
        }

        func <- function(d)
        {
            cols = split_column_to_columns(as.character(d$variable), 3, '.')
            names(cols) = c('operation', 'page.type', 'purpose')
            d = cbind(d, cols)

            d$variable = factor(d$variable, levels=sort(levels(d$variable), 
                decreasing=F))

            d = set_missing_to_default(d, 
                id_cols=c("variable", "filesystem", "subexpname"), val_col="value",
                default_val = 0)

            p = ggplot(d, aes(y=value, x=filesystem, fill=subexpname)) +
                geom_bar(stat='identity', position='dodge') + 
                geom_text(aes(label = value), size=4, 
                          angle=90, hjust=0, position = position_dodge(width=1)) +
                facet_grid(~variable) +
                scale_fill_manual(values=cbPalette, 
                    guide = guide_legend(byrow=T, ncol=3)) +
                ylab("Count") +
                # theme(axis.text.x = element_text(angle=90)) +
                # theme(axis.text.x = element_blank()) +
                theme(legend.position = 'top', legend.direction = 'vertical')

                # coord_flip()
            print(p)
        }

        do_main <- function(expdirs)
        {
            d = load(expdirs)
            d = clean(d)
            func(d)
        }
        do_main(expdirs)
    }

    analyze.dir.gc_cnt.log <- function(dir.paths)
    {
        load.gc.log <- function(fpath)
        {
            # DEBUG victimblock 619 vnv_ratio 1.0
            d = read.table(fpath, header=F,
                colClasses = c("NULL", "character", "NULL", "numeric",      "NULL", "numeric"),
                col.names  = c('',     "gc.type",   "",     'victim.block', "",     'inv.ratio')
                )
            print(head(d))
            return(d)
        }

        load.dir <- function(dir.paths)
        {
            d.all = data.frame()
            for (dir.path in dir.paths) {
                files = list.files(dir.path, recursive = T, 
                    pattern = "gc_cnt.log$", full.names = T)

                for (f in files ) {
                    print(f)
                    d = load_file_from_cache(f, 'load.gc.log')
                    # d = load.gc.log(f)
                    if (nrow(d) == 0) {
                        next
                    }
                    conf = get_conf_by_datafile_path(f)
                    d$fs = conf[['filesystem']]
                    d.all = rbind(d.all, d)
                }

            }
            d.all$valid.ratio = 1-d.all$inv.ratio
            return(d.all)
        }

        func <- function(d)
        {
            d$gc.type = factor(d$gc.type, 
                levels=c("GC_LOG", "GC_SWITCH_MERGE", "GC_FULL_MERGE"))
            d$valid.ratio = 1-d$inv.ratio
            p = ggplot(d, aes(x=valid.ratio, fill=fs)) +
                geom_histogram() +
                facet_grid(fs~gc.type)
            print(p)

            print(table(d[,c('valid.ratio', 'fs')]))
        }

        do_main <- function(dir.paths) 
        {
            d = load.dir(dir.paths)
            func(d)
        }
        do_main(dir.paths)
    }


    analyze.dir.ftlsim.out.count_table <- function(expdirs)
    {
        # This function takes a vector of directories as input
        load <- function(expdirs)
        {
            ret = data.frame()
            for (expdir in expdirs) {
                files = list.files(expdir, recursive = T, 
                   pattern = "ftlsim.out.count_table$", full.names = T)
                for (f in files) {
                    print(f)
                    d = read.csv(f, header=T, sep=';')
                    conf = get_conf_by_datafile_path(f)
                    d$filesystem = as.character(conf['filesystem'])
                    d$interface_level = as.character(conf['interface_level'])
                    d$subexpname = as.character(conf['subexpname'])
                    fileitems = unlist(strsplit(f, "/"))
                    d$file = paste(fileitems[-c(1:4, length(fileitems))], collapse="/")
                    ret = rbind(ret, d)
                }
            }
            return(ret)
        }

        clean <- function(d)
        {
            return(d)
        }

        func <- function(d)
        {
              # count item.name counter.name filesystem                                                   file
# 1  76830      miss        cache       ext4 localresults/tpftl-to-4.2/ext4-tpftl-256-cmtsize-15728
# 2      9       hit        cache       ext4 localresults/tpftl-to-4.2/ext4-tpftl-256-cmtsize-15728
# 3     14       hit        cache       f2fs localresults/tpftl-to-4.2/f2fs-tpftl-256-cmtsize-15728
# 4 148796      miss        cache       f2fs localresults/tpftl-to-4.2/f2fs-tpftl-256-cmtsize-15728 
            d = subset(d, counter.name == 'cache' & item.name %in% c('hit', 'miss'))
            d = arrange(d, as.character(item.name))
            d$item.name = factor(d$item.name)
            d$level.subexp = with(d, interaction(interface_level, subexpname))
            
            d = ddply(d, .(level.subexp, counter.name, filesystem), function(d) {
                      d$item.name = factor(d$item.name, levels=c('hit', 'miss'))
                      d = arrange(d, item.name)
                      d$pos = cumsum(d$count) - 0.5 * d$count
                      return (d)
                   })
            print(d)

            p = ggplot(d, aes(x=level.subexp, y=count, fill=item.name)) +
                geom_bar(stat='identity') +
                geom_text(aes(label = count, y = pos)) +
                facet_grid(counter.name~filesystem) +
                theme(axis.text.x = element_text(angle=90)) 
            print(p)
        }

        func2 <- function(d)
        {
            d = subset(d, counter.name == 'block.info.valid_ratio')
              # count item.name counter.name filesystem                                                   file
# 1  76830      miss        cache       ext4 localresults/tpftl-to-4.2/ext4-tpftl-256-cmtsize-15728
# 2      9       hit        cache       ext4 localresults/tpftl-to-4.2/ext4-tpftl-256-cmtsize-15728
# 3     14       hit        cache       f2fs localresults/tpftl-to-4.2/f2fs-tpftl-256-cmtsize-15728
# 4 148796      miss        cache       f2fs localresults/tpftl-to-4.2/f2fs-tpftl-256-cmtsize-15728 
            d$item.name = as.numeric(as.character(d$item.name))
            maxheight = max(d$count) * 1.2
            p = ggplot(d, aes(x=item.name, y=count)) + 
                geom_text(aes(label=count), hjust=-0.1, angle=90, color='gray') +
                geom_bar(stat='identity') +
                scale_y_continuous(limits=c(0, maxheight)) +
                facet_grid(filesystem~subexpname) +
                xlab('valid.ratio')
            print(p)
        }


        do_main <- function(expdirs)
        {
            d = load(expdirs)
            d = clean(d)
            func2(d)
        }
        do_main(expdirs)
    }

    analyze.dir.bad.block.mappings <- function(expdirs)
    {
        # This function takes a vector of directories as input
        load <- function(expdirs)
        {
            ret = data.frame()
            for (expdir in expdirs) {
                files = list.files(expdir, recursive = T, 
                   pattern = "bad.block.mappings$", full.names = T)
                for (f in files) {
                    print(f)
                    d = read.table(f, header=T)
                    conf = get_conf_by_datafile_path(f)
                    d$filesystem = as.character(conf['filesystem'])
                    d$interface_level = as.character(conf['interface_level'])
                    d$subexpname = as.character(conf['subexpname'])
                    fileitems = unlist(strsplit(f, "/"))
                    d$file = paste(fileitems[-c(1:4, length(fileitems))], collapse="/")
                    ret = rbind(ret, d)
                }
            }
            return(ret)
        }

        clean <- function(d)
        {
            return(d)
        }

        func.ext4 <- function(d)
        {
            p1 = ggplot(subset(d, block_type == 'data_block'), 
                        aes(x = lpn, color = ppn_state)) +
                geom_jitter(alpha = 0.5) + geom_vline(xintercept = c(32785, 36881), color = 'red') +
                geom_vline(xintercept = c(37376, 42495), color = 'blue') +
                geom_vline(xintercept = seq(37376, 42495, 4*2^20/4096)[-1], color = 'green') +
                facet_grid(block_num~., scales = 'free_y') +
                xlab("Logical Page Number") +
                ylab("Flash Block Number") +
                ggtitle("Data block")

            d2 = subset(d, block_type == 'trans_block')
            d2 = ddply(d2, .(block_num), transform, offset = ppn - min(ppn))
            print(d2)
            p2 = ggplot(d2, 
                    aes(y = factor(lpn), x = offset, 
                    color = ppn_state)) +
                geom_jitter(alpha = 0.5) +
                geom_text(aes(label = lpn)) +
                facet_grid(~block_num) +
                xlab("Offset in block (page)") +
                ylab("Virtual Translation Page Number") +
                ggtitle("Trans blocks")

            # grid.arrange(p1, p2)
            print(p1)
            print(p2)
        }

        func.btrfs <- function(d)
        {
            d1 = subset(d, block_type == 'data_block')
            print(head(d1))
            p1 = ggplot(d1, aes(x = lpn, color = ppn_state)) +
                geom_jitter(alpha = 0.5) +
                facet_grid(block_num~., scales = 'free_y') +
                xlab("Logical Page Number") +
                ylab("Flash Block Number") +
                ggtitle("Data block")

            d2 = subset(d, block_type == 'trans_block')
            d2 = ddply(d2, .(block_num), transform, offset = ppn - min(ppn))
            print(d2)
            p2 = ggplot(d2, 
                    aes(y = factor(lpn), x = offset, 
                    color = ppn_state)) +
                geom_jitter(alpha = 0.5) +
                geom_text(aes(label = lpn)) +
                facet_grid(~block_num) +
                xlab("Offset in block (page)") +
                ylab("Virtual Translation Page Number") +
                ggtitle("Trans blocks")

            # grid.arrange(p1, p2)
            # print(p1)
            print(p2)
        }


        func.lpn.hist <- function(d)
        {
            d = subset(d, ppn_state == 'INVALID')
            print(summary(d))
            dcnt = as.data.frame(table(lpn = d$lpn, block_type = d$block_type))
            # dcnt = as.data.frame(table(lpn = d$lpn, block_type = d$block_type))
            dcnt = subset(dcnt, Freq > 0)
            dcnt = arrange(dcnt, block_type, Freq)
            dtop = ddply(dcnt, .(block_type), subset, order(Freq, decreasing = T) < 20 )
            print(dtop, row.names = F)
            p = ggplot(dcnt, aes(x = as.numeric(lpn))) +
                geom_histogram()
            print(p)
        }

        func.simple.print <- function(d)
        {
            classify <- function(lpn)
            {
                if ( 37376 <= lpn && lpn <= 42495 ) {
                    return("FILEDATA")
                } else if (32785 <= lpn && lpn <= 36881 ) {
                    return("JOURNALDATA")
                } else {
                    return("UNKOWN")
                }
            }

            # print(head(d))
            # d$content = sapply(d$lpn, classify)
            # print(subset(d, block_type == 'data_block', select = -file))

            print('-------------block info--------------------')
            # print(subset(d, block_num == 973, select = -file))
            print(d)
        }

        func.check.existence <- function(d)
        {
            is_inside <- function(lpn) {
                datatext = "
                               start;                       elem_type;                            size
                                   0;                BLOCK_GROUP_ITEM;                         4194304
                              131072;                     EXTENT_ITEM;                            4096
                             4194304;                     EXTENT_ITEM;                            4096
                             4194304;                BLOCK_GROUP_ITEM;                         8388608
                             4198400;                     EXTENT_ITEM;                            4096
                             4202496;                     EXTENT_ITEM;                            4096
                             4231168;                     EXTENT_ITEM;                            4096
                             4653056;                     EXTENT_ITEM;                            4096
                             4689920;                     EXTENT_ITEM;                            4096
                             4730880;                     EXTENT_ITEM;                            4096
                             4763648;                     EXTENT_ITEM;                            4096
                             4841472;                     EXTENT_ITEM;                            4096
                             4886528;                     EXTENT_ITEM;                            4096
                             4890624;                     EXTENT_ITEM;                            4096
                             4894720;                     EXTENT_ITEM;                            4096
                             4898816;                     EXTENT_ITEM;                            4096
                             4902912;                     EXTENT_ITEM;                            4096
                             9150464;                     EXTENT_ITEM;                            4096
                            12582912;                BLOCK_GROUP_ITEM;                        33554432
                            46137344;                BLOCK_GROUP_ITEM;                        33554432
                            62914560;                     EXTENT_ITEM;                         4194304
                            67174400;                     EXTENT_ITEM;                         4194304
                            71368704;                     EXTENT_ITEM;                         4194304
                            79691776;                BLOCK_GROUP_ITEM;                        33554432
                            83886080;                     EXTENT_ITEM;                         4194304
                           113246208;                BLOCK_GROUP_ITEM;                        33554432
                           130023424;                     EXTENT_ITEM;                         4194304
    "
                d.ext = read.csv(text=datatext, header=T, sep=';', strip.white=T)
                d.ext = subset(d.ext, elem_type == 'EXTENT_ITEM')

                d.ext$is_inside = with(d.ext, lpn >= start & lpn <= (start+size))
                if ( any(d.ext$is_inside ) ) {
                    return (subset(d.ext, is_inside == T))
                }
            }
            
            lpns = d$lpn

            dd = data.frame()
            for ( lpn in lpns ) {
                tmp = is_inside(lpn)
                dd = rbind(dd, tmp)
            }
            print(dd)
        }

        do_main <- function(expdirs)
        {
            d = load(expdirs)
            d = clean(d)
            # func.btrfs(d)
            # func.lpn.hist(d)
            # func.simple.print(d)
            func.check.existence(d)
        }
        do_main(expdirs)
    }

    analyze.dir.mapping.activity <- function(dir.path)
    {
        load.file <- function(filepath)
        {
            d = read.table(filepath, header=F, 
               colClasses = c("NULL", "character", "character", "character"),
               col.names = c("", "operation", "lba.page", "flash.page"))
            return(d)
        }

        load.dir <- function(dirpath)
        {
            d.all = data.frame()
            files = get_file_list(dir.path)
            for ( f in files ) {
                print(f)
                # d = load.file(f)
                d = load_file_from_cache(f, "load.file")

                conf = get_conf_by_datafile_path(f)
                npage.per.block = conf[['flash_npage_per_block']]
                d$npage.per.block = npage.per.block
                d$file = f
                d$hash = substr(digest(f, algo="md5", serialize=F), 0, 4)
                d$fs = conf[['filesystem']]

                d.all = rbind(d.all, d)
            }
            return(d.all)
        }

        clean <- function(d)
        {
            d = subset(d, operation == 'user_write' & flash.page != 'None')
            d$lba.page = as.numeric(d$lba.page)
            d$flash.page = as.numeric(d$flash.page)
            return(d)
        }

        func <- function(d)
        {
            d$lba.off = d$lba.page %% d$npage.per.block
            d$flash.off = d$flash.page %% d$npage.per.block
            d$aligned = with(d, lba.off == flash.off)
            d$fs = paste(d$fs, d$hash, sep='.')

            # print(table(d[, c('file', 'aligned')]))
            d.freq = as.data.frame(table(d[, c('fs', 'aligned')]))
            d.freq$aligned = d.freq$aligned == "TRUE"
            d.freq$aligned = ifelse(d.freq$aligned, "Aligned", "Not Aligned")
            p = ggplot(d.freq, aes(x = fs, y=Freq, fill=aligned)) +
                geom_bar(aes(order=desc(aligned)), stat='identity', position='stack') +
                ylab("Count") +
                ggtitle("Aligned/Not Aligned writes")
            print(p)
        }
        
        get_file_list <- function(dir.path)
        {
            files = list.files(dir.path, recursive = T, 
                pattern = "hybridmapping.log$", full.names = T)
            return(files)
        }

        do_main <- function(dir.path)
        {
            d = load.dir(dir.path)
            d = clean(d)
            func(d)
        }

        do_main(dir.path)
    }

    local_main <- function()
    {
        # analyze.dir.ftilsim.out("~/datahouse/sequential14")
        # analyze.dir.gc.log("~/datahouse/sequential14")
        # analyze.dir.events.for.ftlsim("~/datahouse/sequential14")
        # analyze.dir.stats(c("~/datahouse/sequential.btrfs.nocow",
                            # "~/datahouse/sequential13"))
        # analyze.dir.gc_cnt.log.put_and_count.stats(
           # c("~/datahouse/backwards.blktrace.right"))
        # analyze.dir.gc_cnt.log.put_and_count.stats(
           # c("~/datahouse/sequential.blktrace.right"))
        # analyze.dir.ftlsim.out.stats(c("~/datahouse/sequential.blktrace.right.btrfs", 
                                       # "~/datahouse/sequential.blktrace.right"))
        # analyze.dir.gc.log(c("~/datahouse/sequential.blktrace.right.btrfs"))
        # analyze.dir.ftlsim.out.stats(c("~/datahouse/improved.gc2/", 
                                       # "~/datahouse/sequential.blktrace.right"))

        # analyze.dir.ftlsim.out.stats(c("~/datahouse/mapping.activity"))
        # analyze.dir.mapping.activity("~/datahouse/mapping.activity")
        # analyze.dir.gc_cnt.log(c("~/datahouse/mapping.activity")) # ** USEFUL **

        # higher.mark
        # dirpath = "~/datahouse/higher.mark"
        # analyze.dir.ftlsim.out.stats(dirpath)

        # analyze.dir.ftilsim.out("~/datahouse/backwards.impr.gc")
        # analyze.dir.ftlsim.out.stats("~/datahouse/backwards.impr.gc")

        # suite("~/datahouse/backwards.impr.gc")
        # suite("~/datahouse/sequential.nojournal")
        # suite("~/datahouse/backwards.nojournal")
        # analyze.dir.ftlsim.out.stats(c("~/datahouse/backwards.impr.gc",
                                       # "~/datahouse/backwards.nojournal"))

        # analyze.dir.ftilsim.out("~/datahouse/dftl")
        # analyze.dir.ftlsim.out.stats("~/datahouse/dftl")

        # suite("~/datahouse/compare.caches/")
        # suite("~/datahouse/fs.and.dftl/")
        # suite("~/datahouse/localresults/bricks")
        # suite("~/datahouse/localresults/bricks-large-chunk")
        # suite("~/datahouse/localresults/bricks-10mb-chunk")
        # suite("~/datahouse/localresults/bricks-newtags3/")
        # suite("~/datahouse/localresults/bricks-2-iter/")
        # suite("~/datahouse/localresults/bricks-0.8-highwatermark/")
        # suite("~/datahouse/localresults/meeting-tmp")
        # suite("~/datahouse/localresults/meeting-hotcold-betterbricks-optimal")
        # suite("~/datahouse/localresults/meeting-hotcold-optimal-2iter")
        # suite("~/datahouse/localresults/meeting-hotcold-2iter")
        # suite("~/datahouse/localresults/meeting-hotcold-4mb-chunk")
        # suite("~/datahouse/localresults/debugdftl2new")
        # suite("~/datahouse/localresults/debugtpftl")
        # suite("~/datahouse/localresults/tpftl-to-4.2")
        # suite("~/datahouse/localresults/tpftl-compare-page-extent")
        # suite("~/datahouse/localresults/tpftl-batch")
        # suite("~/datahouse/localresults/tpftl-right-hotness")
        suite(c("~/datahouse/localresults/tpftl-almost", "~/datahouse/localresults/study-btrfs-20iter/btrfs-dftl2-256-cmtsize-15728-discard.no.gc..improved.decider.sepappend-2015-08-18-09-28-48"))
        # suite("~/datahouse/localresults/investigate/")
        # suite("~/datahouse/localresults/everyone-1gc/")

        # suite("~/datahouse/localresults/study-btrfs-20iter/")
        # suite("~/datahouse/localresults/study-btrfs-20iter/btrfs-dftl2-256-cmtsize-15728-recording-2015-08-17-02-24-22")
        # suite("~/datahouse/localresults/study-btrfs-20iter/btrfs-dftl2-256-cmtsize-15728-deciderimproved-2015-08-17-22-02-12")
        # suite("~/datahouse/localresults/study-btrfs-20iter/btrfs-dftl2-256-cmtsize-15728-sepappend.baddecider-2015-08-18-03-44-05")
        # suite("~/datahouse/localresults/study-f2fs")
        # suite("~/datahouse/localresults/study-ext4-v2")

        # suite("~/datahouse/localresults/study-ext4-weird")
        # suite("~/datahouse/localresults/study-ext4-tpftl")


        # For meeting 07/10

        # Sequential
        # suite("~/datahouse/mapping.activity")

        # Backwards
        # suite("~/datahouse/backwards.impr.gc")

        # Random
        # suite("~/datahouse/random")
    }

    suite <- function(dirpath)
    {
        # analyze.dir.ftilsim.out(dirpath)
        analyze.dir.ftlsim.out.stats(dirpath)
        # analyze.dir.gc_cnt.log(dirpath) # ** USEFUL **
        # analyze.dir.mapping.activity(dirpath)
        # analyze.dir.ftlsim.out.count_table(dirpath)
        # analyze.dir.events.for.ftlsim2(dirpath)
        # analyze.dir.bad.block.mappings(dirpath)
    }

    local_main()
}


btrfs.discard.analysis <- function()
{
    load.file <- function(filepath)
    {
        d = read.table(filepath, header=F,
            col.names = c("operation", "offset", "size"))
        return(d)
    }

    load.dir <- function(dir.path)
    {
        d.all = data.frame()
        dirs = dir(dir.path, include.dirs = T, full.names = T)

        for ( dirpath in dirs ) {
            f = paste(dirpath, 'blkparse-events-for-ftlsim.txt', sep='/')
            print(f)
            if ( ! grepl('discard', f) ) {
                print('skip')
                next
            } else {
                print('keep')
            }

            d = load.file(f)
            # d = load_file_from_cache(f, "load.file")

            conf = get_conf_by_datafile_path(f)
            d$filesystem = as.character(conf['filesystem'])
            d$subexpname = as.character(conf['subexpname'])

            d.all = rbind(d.all, d)
        }
        return(d.all)
    }


    clean <- function(d)
    {
        return(d)
    }

    func <- function(d)
    {
        d = subset(d, subexpname == 'discard+.sdd+.autodefrag-')
        print(head(d))
        print(nrow(d))
        # d$seqid = seq_along(d$operation)
        d = ddply(d, .(subexpname), transform, seqid = seq_along(operation))
        d$offset = d$offset / 2^20
        d$size = d$size / 2^20

        d = subset(d, operation == 'discard')
        d$seqid = factor(d$seqid)
        p = ggplot(d) +
            geom_segment(aes(x = offset, xend = offset + size, 
                y = seqid, yend = seqid, color = operation), size = 5) + 
            scale_x_continuous(breaks = seq(0, 10000, 4)) +
            theme(axis.text.x = element_text(angle=90))  +
            facet_grid(subexpname~.) +
            coord_flip()



        print(p)
    }

    do_main <- function()
    {
        d = load.dir("~/datahouse/localresults/study-btrfs-0819a/")
        d = clean(d)
        func(d)
    }
    do_main()
}


btrfs.discard.analysis.wftrace <- function()
{
    load.file <- function(filepath)
    {
        d = read.table(filepath, header=F,
            col.names = c("operation", "offset", "size"))
        return(d)
    }

    clean <- function(d)
    {
        return(d)
    }

    func <- function(d)
    {
        print(head(d))
        print(nrow(d))
        d$offset = d$offset / 2^20
        d$size = d$size / 2^20

        d$seqid = seq_along(d$operation)
        # d$seqid = factor(d$seqid)
        p = ggplot(d) +
            geom_segment(aes(x = offset, xend = offset + size, 
                y = seqid, yend = seqid, color = operation), size = 5, alpha=0.5) + 
            scale_x_continuous(breaks = seq(0, 10000, 4)) +
            theme(axis.text.x = element_text(angle=90))  +
            coord_flip()



        print(p)
    }

    do_main <- function()
    {
        d = load.file(
                paste("~/datahouse/localresults/blkparse/fstrim2-2015-08-25-00-40-46-btrfs-dftl2-256-cmtsize-15728",
                      "blkparse-events-for-ftlsim.txt", sep='/'))
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
    # explore.sim.results()
    # explore.sim.results.too.new()
    # explore.sim.results.alter.table.for.meeting.0702()
    # explore.sim.results.for.meeting.0702()
    # explore.mywl()
    # explore.stats()
    # explore.function.hist()

    # explore.stack()
    btrfs.discard.analysis.wftrace()
}
main()
