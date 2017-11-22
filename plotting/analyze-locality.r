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
    THISFILE     ='doraemon/analyze-locality.r'
    setwd(WORKDIRECTORY)
    source(THISFILE)
}


SubExpHitMissRatio <- setRefClass("SubExpHitMissRatio",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            confjson = create_configjson(subexppath=subexppath)
            recjson = RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            pagesize=as.numeric(confjson$get_page_size())
            blocksize=as.numeric(confjson$get_block_size())
            
            return(list(
                blocksize=confjson$get_block_size(),
                pagesize=confjson$get_page_size(),
                filesystem=confjson$get_filesystem(),
                workload_class=confjson$get_workload_class(),
                mapping_cache_bytes=confjson$get_mapping_cache_bytes(),
                hit_cnt=recjson$get_hit_count(),
                miss_cnt=recjson$get_miss_count(),
                mapped_data_bytes=confjson$get_cache_mapped_data_bytes(),
                lbabytes=confjson$get_lbabytes(),

                appname=confjson$get_testname_appname(),
                fs_discard=confjson$get_appmix_fs_discard(),
                testname=confjson$get_testname(),
                rw=confjson$get_testname_rw(),
                pattern=confjson$get_testname_pattern()
                ))
        }
    )
)
 
MissRatioCurve <- setRefClass("MissRatioCurve",
    fields = c('expname', 'df'),
    methods = list(
        plot_miss_ratio_curve = function(exp_rel_path)
        {
            d = get_data(exp_rel_path)

            d = transform(d, miss_ratio = miss_cnt / (miss_cnt + hit_cnt))
            d = transform(d, cache_coverage = round(mapped_data_bytes / lbabytes, 2))
            d = remove_dup(d, c('cache_coverage', 'workload_class', 
                                'filesystem'))

            d = transform(d, cache_coverage = factor(cache_coverage))

            print(d)

            p = ggplot(d, aes(x=cache_coverage, y=miss_ratio, 
                              color=filesystem)) +
                geom_line(aes(group=filesystem)) +
                geom_point() +
                facet_grid(~workload_class) +
                # scale_y_continuous(limits=c(0, 1)) +
                xlab('Cache Coverage') +
                ylab('Miss Ratio') +
                scale_color_manual(
                   values=get_color_map2(c('btrfs', 'ext4', 'f2fs', 'xfs'))) +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
            print(p)
            save_plot(p, 'app-mrc', save=T, h=3)
        },
        get_data = function(exp_rel_path)
        {
            d = aggregate_subexp_list_to_dataframe(exp_rel_path, 
                                               SubExpHitMissRatio)

            d = cols_to_num(d, c('hit_cnt', 'miss_cnt', 'mapping_cache_bytes',
                                 'lbabytes', 'mapped_data_bytes'
                                 ))
            return(d)
        }
    )
)


c_____PAPER_MISS_RATIO_CURVE______________ <- function(){}


SubExpHitMissRatioNEW <- setRefClass("SubExpHitMissRatioNEW",
    fields = list(subexppath="character"),
    methods = list(
        run = function()
        {
            confjson = create_configjson(subexppath=subexppath)
            recjson = RecorderJson(
                  filepath=paste(subexppath, 'recorder.json', sep='/'))

            pagesize=as.numeric(confjson$get_page_size())
            blocksize=as.numeric(confjson$get_block_size())
            

            return(list(
                blocksize=confjson$get_block_size(),
                pagesize=confjson$get_page_size(),
                filesystem=confjson$get_filesystem(),
                workload_class=confjson$get_workload_class(),
                mapping_cache_bytes=confjson$get_mapping_cache_bytes(),
                hit_cnt=recjson$get_hit_count(),
                miss_cnt=recjson$get_miss_count(),
                mapped_data_bytes=confjson$get_cache_mapped_data_bytes(),
                lbabytes=confjson$get_lbabytes(),
                appname=confjson$get_testname_appname(),
                fs_discard=confjson$get_appmix_fs_discard(),
                testname=confjson$get_testname(),
                rw=confjson$get_testname_rw(),
                pattern=confjson$get_testname_pattern()
                ))
        }
    )
)
 
 
MissRatioCurveNEW <- setRefClass("MissRatioCurveNEW",
    fields = c('expname', 'df'),
    methods = list(
        plot_miss_ratio_curve = function(exp_rel_path)
        {
            d = get_data(exp_rel_path)

            d = transform(d, miss_ratio = miss_cnt / (miss_cnt + hit_cnt))
            d = transform(d, cache_coverage = round(mapped_data_bytes / lbabytes, 2))
            d = transform(d, cache_coverage = factor(cache_coverage))

            d = rename_pattern_levels(d)
            d = shorten_appnames(d)
            d = set_apprw(d)

            p = ggplot(d, aes(x=cache_coverage, y=miss_ratio, 
                              color=filesystem)) +
                geom_line(aes(group=filesystem, linetype=filesystem,
                              size=filesystem
                              )) +
                # geom_point() +
                facet_grid(pattern~apprw) +
                # scale_y_continuous(limits=c(0, 1)) +
                xlab('Cache Coverage') +
                ylab('Miss Ratio') +
                scale_color_manual(values=get_fs_color_map(NULL)) +
                scale_linetype_manual(values=get_fs_linetype_map()) +
                scale_size_manual(values=get_fs_size_map()) +
                scale_y_continuous(breaks=c(0, 0.2, 0.4)) +
                theme_zplot() +
                theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) +
                theme(legend.position=c(0.5, 0.93), 
                      legend.background=element_blank(),
                      legend.direction="horizontal",
                      legend.title=element_blank())
                # geom_line(aes(color=filesystem))
            print(p)
            save_plot(p, 'app-mrc', save=T, w=6.6, h=1.8)
        },
        set_apprw = function(d)
        {
            d$rw = revalue(d$rw, c('r'='read', 'w'='write'))
            d = transform(d, apprw=interaction(appname, rw, sep='\n', lex.order = TRUE))
            # d$apprw = revalue(d$apprw, c('leveldb\nwrite'='\nwrite',
                                         # 'rocksdb\nwrite'=' \nwrite',
                                         # 'sqlite-rb\nwrite'='  \nwrite',
                                         # 'sqlite-wal\nwrite'='   \nwrite',
                                         # 'varmail\nwrite'='     \nwrite'
                                         # ))
            return(d)
        },
        get_data = function(exp_rel_path)
        {
            if (cache_exists(exp_rel_path)) {
                print('Using cache')
                print(cache_path(exp_rel_path))
                load(cache_path(exp_rel_path))
            } else {
                print('NO cache')
                d = aggregate_subexp_list_to_dataframe(exp_rel_path, 
                                                   SubExpHitMissRatioNEW)

                d = cols_to_num(d, c('hit_cnt', 'miss_cnt', 'mapping_cache_bytes',
                                     'lbabytes', 'mapped_data_bytes'
                                     ))
                save(d, file=cache_path(exp_rel_path))
            }
 
           return(d)
        },
        cache_path = function(exp_rel_path)
        {
            exp_rel_path = gsub('/', '', exp_rel_path)
            exp_rel_path = paste(exp_rel_path, collapse='-')
            cache_path = paste(digest(exp_rel_path), 'miss-ratios', 'rdata', sep='.')

            return(cache_path)
        },
        cache_exists = function(exp_rel_path)
        {
            path = cache_path(exp_rel_path)
            return( file.exists(path) )
        }
 
    )
)



c_____________________________________ <- function(){}

plot_mrc_by_cache <- function()
{
    load('app-miss-ratio-curves-do-not-delete.rdata')

    d = subset(d, filesystem != 'btrfs')

    p = ggplot(d, aes(x=cache_coverage, y=miss_ratio, 
                      color=filesystem)) +
        geom_line(aes(group=filesystem), size=0.4) +
        # geom_point() +
        facet_grid(~workload_class) +
        # scale_y_continuous(limits=c(0, 1)) +
        xlab('Cache Coverage') +
        ylab('Miss Ratio') +
        scale_color_manual(values=get_fs_color_map()) +
        theme_zplot() +
        theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5)) 
        # geom_line(aes(color=filesystem))
    print(p)
    save_plot(p, 'app-mrc', save=T, h=2.5)
}
 


paper_plot <- function()
{
    miss_curve = MissRatioCurve()
    miss_curve$plot_miss_ratio_curve(c(
                                       'leveldb-locality-0.1cover',
                                       'leveldb-locality-addition',
                                       'sqlite-locality-0.5-1-cover-nobtrfs',
                                       'sqlite-on-dora1-0.01-0.05-0.1',
                                       'sqlite-locality-doara1-btrfs',
                                       'varmail-locality-0.1-0.05cover',
                                       'varmail-locality-addition-go-3',
                                       'varmail-locality-addition-0.01-coverage'
                                       ))
}




main <- function()
{

    miss_curve = MissRatioCurveNEW()
    miss_curve$plot_miss_ratio_curve(c(
                                       "locality-rocksdb-filesim",
                                       "locality-leveldb-filesim",
                                       "locality-sqlitewal-tracesim",
                                       "locality-sqliterb-tracesim",
                                       "locality-varmail-filesim", # stop on 1GB

                                       # small cache coverage
                                       "localitysmall-sqliterb-tracesim",
                                       # "localitysmall-varmail-tracesim", # stop on 512MB
                                       "varmail-locality-newSTOP1GB",
                                       "localitysmall-rocksdb-tracesim",
                                       "localitysmall-leveldb-tracesim",
                                       "localitysmall-sqlitewal-tracesim"
                                       ))






    # paper_plot()
    # return()

    # plot_mrc_by_cache()

}

main()















