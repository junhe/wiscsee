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
    THISFILE     ='doraemon/explore.r'
    setwd(WORKDIRECTORY)
    source(THISFILE)
}

c______LPN_DEBUG_____________ <- function(){}

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
            d = transform(d, lpn_sorted = seq_along(count))

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
                # lvs[non_metadata] = 'Data-region'
                print(lvs)

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
        plot_sem = function(d, ratio, position)
        {
            d = get_top_or_bottom(d, ratio, position)

            d = transform(d, n_pages = 1)
            d.n_pages = aggregate(n_pages~sem, data=d, sum)
            d.n_access = aggregate(count~sem, data=d, sum)
            d.stats = merge(d.n_pages, d.n_access)
            d.stats = transform(d.stats, avg_acc_count = count / n_pages)
            print(d.stats)

            print(head(d, 4))
        }
    )
)

LpnSemStats <- setRefClass("LpnSemStats",
    fields = list(expname='character'),
    methods = list(
        plot = function(exp_rel_path)
        {
            d = aggregate_subexp_dataframes(exp_rel_path, SubExpLpnSem)

            d = cols_to_num(d, c('read', 'write', 'discard', 'count'))
            d = transform(d, filesystem = factor(filesystem, 
                     levels=c('btrfs', 'ext4', 'f2fs', 'xfs')))

            d = subset(d, sem != 'Journal')
            # d = subset(d, sem != 'data.db')

            dd = get_max_coord(d)
            print(dd)
            
            d$benchname = factor(d$benchname, levels = sort(levels(d$benchname)))

            d = transform(d, lpn = seq_along(lpn))

            p = ggplot(d, aes(x=lpn*2*KB/MB, y=count, color=filesystem))+
                geom_line() +
                facet_grid(~benchname) +
                scale_color_grey(start=0.7, end=0) +
                # scale_x_continuous(breaks=c(0, 0.5, 1),
                                   # labels=as.character(c(0, 0.5, 1)))+
                scale_y_log10() +
                theme_zplot()
            print(p)
        },
        get_max_coord = function(d)
        {
            dd = ddply(d, .(benchname, filesystem), subset, lpn_ratio == min(lpn_ratio)) 

            return(dd)
        }
    )
)


main <- function()
{

    semplot = LpnSemStats()
    # semplot$plot('leveldb-lpncount-classify')
    # semplot$plot('ext4-3apps-lpnsem-lpncount')

    semplot$plot('ext4-apps-lpn-count-paddingbugfixed/subexp--7742714436798700426-ext4-09-16-14-40-34--8560570262031721507') #<- every body good long run
    # semplot$plot('f2fs-correct-classification-long') # data for the paper
    # semplot$plot(c('xfs-3app-with-filerange/subexp--2129757659727960388-xfs-09-18-22-02-32--816041259488816439'))


}
main()

