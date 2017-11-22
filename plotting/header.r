KB = 2^10
MB = 2^20
GB = 2^30
TB = 2^40

SEC = 10^9
MILISEC = 10^6
MICROSEC = 10^3
NANOSEC = 1

cbPalette <- c("#89C5DA", "#DA5724", "#74D944", "#CE50CA", "#3F4921", "#C0717C", "#CBD588", "#5F7FC7", 
    "#673770", "#D3D93E", "#38333E", "#508578", "#D7C1B1", "#689030", "#AD6F3B", "#CD9BCD", 
    "#D14285", "#6DDE88", "#652926", "#7FDCC0", "#C84248", "#8569D5", "#5E738F", "#D1A33D", 
    "#8A7C64", "#599861")

PAPER_COL_WIDTH = 3.2

save_plot <- function(p, filename, save, w=3.2, h=2.1, ext='pdf')
{
    print(filename)
    filename = paste(filename, '.', ext, sep='')
    if (save == TRUE) {
        ggsave(filename, plot=p, height=h, width=w) 
    }
}

size_unit_for_human <- function(bytes)
{
    if (bytes == 0)
        return ('0')

    size_names = c('bytes', 'KB', 'MB', 'GB', 'TB')
    size_boundaries = c(1, KB, MB, GB, TB)
    idx = findInterval(bytes, size_boundaries)
    low_bound = size_boundaries[idx]
    low_bound_name = size_names[idx]
    bytes_new_unit = bytes / low_bound

    return(paste(bytes_new_unit, low_bound_name))
}

load_event_file <- function(fpath)
{
    d = read.table(fpath, header = F)
    if (ncol(d) == 8) { 
        names(d) = c('pid', 'action', 'operation', 'offset', 'size', 'timestamp', 'pre_wait_time',
                     'sync')
    } else {
        names(d) = c('pid', 'operation', 'offset', 'size', 'timestamp', 'pre_wait_time',
                     'sync')
    }
    return(d)
}

to_human_unit <- function(vec)
{
    named_vec = sapply(vec, size_unit_for_human)
    return(named_vec)
}

to_human_factor <- function(vec)
{
    map_nums = sort(unique(vec))
    map_names = to_human_unit(map_nums)

    named_vec = to_human_unit(vec)
    new_factor = factor(named_vec, levels=map_names)
    return(new_factor)
}

theme_zplot <- function()
{
    update_geom_defaults("bar",   list(fill = "black")) # set bar color to black

    fontsize = 8 # USENIX font size is 10pts, 8 is slightly smaller and looks good
    linesize = 0.3 # 0.3 is about the width of font lines, looks good
    # the above only works on paper is you don't scale the graph
    # you should set figure width to 3.2 inches to match the column size of two-column
    # paper

    t = theme_classic(base_size = fontsize, base_family = 'ArialMT') +
        theme(plot.margin = unit(c(0, 0, 0, 0), 'cm')) +
        theme(axis.text = element_text(size = fontsize)) +
        # theme(axis.title.y=element_text(vjust=1.5)) +
        # theme(axis.title.x=element_text(vjust=0)) +
        theme(axis.line = element_line(size = linesize, colour = "black")) +
        theme(axis.ticks = element_line(size = linesize, color = "black")) +
        theme(plot.title = element_text(size = fontsize)) +
        theme(legend.text = element_text(size = fontsize)) +
        theme(strip.background = element_rect(size=0, color='white')) +
        theme(strip.text = element_text(size = fontsize)) +
        theme(legend.position='top', legend.title=element_blank()) +
        theme(legend.key.size = unit(0.4, 'cm')) +
        theme(legend.margin = unit(0, 'cm'))
    
    return(t)
}

scale_zplot <- function()
{
    s = list(scale_color_grey(start=0),
        scale_fill_grey(start=0))

    return(s)
}


get_exp_path <- function(expdir)
{
    p = paste("/Users/junhe/datahouse/localresults/", expdir, sep='')
    return(p)
}

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
 
    l = strsplit(as.character(char_vec), splitchar, fixed=T)
    ret = do.call('rbind', lapply(l, fill_na))
    ret = as.data.frame(ret)
    return(ret)
}

get_file_paths_recursively <- function(dirpaths, filename)
{
    all_files = c()
    for (dirpath in dirpaths) {
        files = list.files(dirpath, recursive = T, 
           pattern = paste("^", filename, "$", sep = ""), 
           full.names = T)
        all_files = append(all_files, files)
    }
    return(all_files)
}

sort_num_factor <- function(f)
{
    f = factor(f)
    nums = as.numeric(levels(f))
    nums = as.character(sort(nums))
    return( factor(f, levels = nums) )
}

read_json <- function(jsonpath)
{
    doc <- fromJSON(txt=jsonpath)
    return(doc)
}

get_dir_path <- function(path)
{
    elems = unlist(strsplit(path, '/'))
    n = length(elems)
    return(paste(head(elems, n-1), collapse='/'))
}

get_conf_by_datafile_path <- function(datafilepath)
{
    confpath = datafile_to_conffile(datafilepath)
    return( read_json(confpath) )
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

# This function is for the case that when plotting with ggplot,
# some bar is missing so other bars at the same location become
# bigger.
# See evernote title: "fill in missing values" for more details
# id_cols, val_col are characters
set_missing_to_default <- function(d, id_cols, val_col, default_val)
{
    # remove unnecessary columns, you have to do this
    d = d[, c(id_cols, val_col)]

    level_list = lapply(as.list(d[, id_cols]), unique)
    # print(level_list)
    d.temp = expand.grid(level_list)
    # print(d.temp)
    d.new = merge(d, d.temp, all=T)
    # print(d.new)
    d.new[, val_col] = ifelse(is.na(d.new[, val_col]), default_val, 
                              d.new[, val_col])
    # print(d.new)
    return(d.new)
}

null_is_zero <- function(value)
{
    if (is.null(value)) {
        return(0)
    } else {
        return(value)
    }
}

trim <- function (x) gsub("^\\s+|\\s+$", "", x)

sort_factor_by_alpha <- function(f)
{
    f2 = factor(f, levels=sort(levels(f)))
    return(f2)
}


# Example: cols_to_num(d, c("col1", "col2"))
cols_to_num <- function(d, colnames)
{
    existing_cols = names(d)
    for(colname in colnames) {
        print(colname)
        if (!colname %in% existing_cols) {
            print(paste('Warning:', colname, 'not exist'))
            next
        }
        d[, colname] = as.numeric(as.character(d[, colname]))
    }
    return(d)
}

decor_factor_levels <- function(pre, vec, post='', sep=' ')
{
    fac = factor(vec)
    levels(fac) = paste(pre, levels(fac), post='', sep)
    return(fac)
}

decor_factor_levels_2 <- function(d, pre, colname, post, sep=' ')
{
    d[, colname] = decor_factor_levels(pre, d[, colname], post, sep)
    return(d)
}

merge.all <- function(x, y) {
    merge(x, y, all=TRUE)
}

aggregate_subexp_list_to_dataframe2 <- function(exp_rel_path, sub_analyzer)
{
    result_list = list()
    for (path in exp_rel_path) {
        subexpiter = SubExpIter(exp_rel_path=exp_rel_path)
        result = subexpiter$iter_each_subexp(sub_analyzer)
        result_list = append(result_list, result)
    }
    print(result_list)
    my_list <<- result_list
    d = Reduce(merge.all, result_list)
    print(d)
    d = as.data.frame(d)
    print('dataframe')
    print(d)
    return(d)
}

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

# example:
# aggregate_subexp_results('myexp1', SubExpFioResult) 
aggregate_subexp_results <- function(exp_rel_path, sub_analyzer)
{
    results = list()
    for (path in exp_rel_path) {
        subexpiter = SubExpIter(exp_rel_path=path)
        result = subexpiter$iter_each_subexp(sub_analyzer)
        results = append(results, result)
    }

    results = lapply(results, unlist)
    d = do.call('rbind', results)
    d = data.frame(d)

    return(d)
}

aggregate_subexp_dataframes <- function(exp_rel_path, sub_analyzer)
{
    subexpiter = SubExpIter(exp_rel_path=exp_rel_path)

    result = subexpiter$iter_each_subexp(sub_analyzer)
    # result = lapply(result, unlist)
    d = do.call('rbind', result)
    d = data.frame(d)
    return(d)
}

create_configjson <- function(subexppath)
{
    confjson = ConfigJson(para=paste(subexppath, 'config.json', sep='/'))
    return(confjson)
}

load_gclogparsed = function(filepath)
{
    d = read.csv(file=filepath, header=T, sep=';')
    return(d)
}


ggplot_common_addon <- function()
{
    p = theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5))
    return(p)
}

remove_dup <- function(d, by_col)
{
    dup = duplicated(d[, by_col])
    d = d[!dup, ]

    return(d)
}

scale_fill_grey_dark <- function()
{
    return(scale_fill_grey(start=0))
}

scale_color_grey_dark <- function()
{
    return(scale_color_grey(start=0))
}



global_plots = list()
batch_plots = TRUE

print_plot <- function(p)
{
    if (batch_plots == TRUE) {
        print('batched')
        global_plots <<- append(global_plots, list(p))
    } else {
        print(p)
    }
}


print_global_plots <-function(save_to_file=FALSE, w=10, h=10)
{
    n_plots = length(global_plots)
    if ( n_plots == 0) 
        return()

    print('n_plots')
    print(n_plots)
    n_per_cavas = 16
    remaining = n_plots
    start = 1
    loop_i = 0
    while (remaining > 0) {
        m = min(n_per_cavas, remaining)
        
        if (save_to_file == TRUE) {
            filename = paste('global-plots-', loop_i, '.pdf', sep='')
            print(filename)
            pdf(file = filename, width = w, height = h)
        }

        do.call(grid.arrange, global_plots[start:(start+m-1)])

        if (save_to_file == TRUE) {
            dev.off()
            print('Printed to file')
        } else {
            print('Printed to IDE')
        }

        start = start + m
        remaining = remaining - m
        loop_i = loop_i + 1
        print(paste('printed', m, 'on this cavas'))

    }
}

purge_env <- function()
{
    rm(list = ls())
}

get_color_map2 <- function(tags)
{
    map = c()
    idx = 1 
    for (tag in tags) {
        map[tag] = cbPalette[idx]
        idx = idx + 1
    }
    return(map)
}

get_fs_color_map <- function(tags)
{
    # colors = c("#4D4D4D", "#AEAEAE", "#E6E6E6") #1
    # colors = gray.colors(3, start=0.9, end=0) #2
    colors = gray.colors(3, start=0.7, end=0) #3
    # colors = gray.colors(3, start=0.5, end=0) #4
    names(colors) = c('ext4', 'f2fs', 'xfs')
    return(colors)
}

get_two_grays <- function()
{
    return( gray.colors(2, start=0.7, end=0) )
}
get_perf_type_color_map <- function()
{
    two_colors = get_two_grays() 
    colors = c(two_colors, two_colors[2])
    names(colors) = c('Immediate', 'Sustainable', 'Regular.Read')
    return(colors)
}

get_fs_linetype_map <- function(tags)
{
    # linetypes = c('dashed', 'solid', 'solid')
    linetypes = c('solid', 'solid', 'solid')
    names(linetypes) = c('ext4', 'f2fs', 'xfs')
    return(linetypes)
}


get_fs_size_map <- function(tags)
{
    sizes = c(0.8, 0.3, 0.3)
    names(sizes) = c('ext4', 'f2fs', 'xfs')
    return(sizes)
}


mv_screen_shot <- function()
{
    system('python /Users/junhe/reuse/scripts/mv_pic_rmarkdown.py')
}


white_ribbon <- function()
{
    fontsize = 8 # USENIX font size is 10pts, 8 is slightly smaller and looks good
    linesize = 0.3 # 0.3 is about the width of font lines, looks good

    t = theme_classic(base_size = fontsize, base_family = 'ArialMT') +
        theme(plot.margin = unit(c(0,0,0,0), 'cm')) +
        theme(axis.text = element_blank()) +
        theme(axis.title.y=element_blank()) +
        theme(axis.title.x=element_blank()) +
        theme(axis.line = element_blank()) +
        theme(axis.ticks = element_blank()) +
        theme(plot.title = element_blank()) +
        theme(panel.background = element_blank())+
        theme(plot.background = element_blank())+
        theme(legend.text = element_text(size = fontsize)) +
        theme(strip.background = element_rect(size=0, color='white')) +
        theme(strip.text = element_blank()) +
        theme(legend.position='top', legend.title=element_blank())

    a = rep(c(1,2), 8)
    d = data.frame(yy = a, xx=seq_along(a))
    pp = ggplot(d) + geom_ribbon(aes(x=xx, ymin=yy, ymax=yy+2), fill='white') + t
    g = ggplotGrob(pp)
    return(g)
}

print_debug_warning <- function()
{
    print('WARNING: DEBUGGING CODE IS RUNNING. MAY PRODUCE WRONG RESULTS!!')
}


order_fs_levels <- function(d)
{
    d = transform(d, filesystem = 
                  factor(filesystem, levels=c('ext4', 'f2fs', 'xfs')))
    return(d)
}

order_pattern_levels <- function(d)
{
    d = transform(d, pattern = 
            factor(pattern, levels=c('small', 'large', 'seq', 'rand', 'mix')))

    return(d)
}

order_rw_levels <- function(d)
{
    d = transform(d, rw = 
            factor(rw, levels=c('r', 'w')))

    return(d)
}

rename_sqlite <- function(d)
{
    lvs = levels(d$appname)
    lvs = gsub('sqliteRB', 'sqltRB', lvs, fixed=T)
    lvs = gsub('sqliteWAL', 'sqltWAL', lvs, fixed=T)

    levels(d$appname) = lvs
    return(d)
}

rename_ftlname <- function(d)
{
    lvs = levels(d$FTL)
    lvs = plyr::revalue(lvs, c('dftldes'='Page', 'nkftl2'='Hybrid'))
    levels(d$FTL) = lvs
    return(d)
}



shorten_appnames <- function(d)
{

    d = transform(d, 
            appname = factor(appname, 
             levels=c('leveldb', 'rocksdb', 'sqliteRB', 'sqliteWAL', 'varmail')))

    lvs = levels(d$appname)
    lvs = gsub('sqliteRB',  'sqlite-rb', lvs, fixed=T)
    lvs = gsub('sqliteWAL', 'sqlite-wal', lvs, fixed=T)
    lvs = gsub('leveldb', 'leveldb', lvs, fixed=T)
    lvs = gsub('rocksdb', 'rocksdb', lvs, fixed=T)
    lvs = gsub('varmail', 'varmail', lvs, fixed=T)

    levels(d$appname) = lvs
    return(d)
}




rename_pattern_levels <- function(d)
{
    lvs = levels(d$pattern)

    lvs = revalue(lvs, c('large'='rand/L'))
    lvs = revalue(lvs, c('rand'= 'rand/L'))
    lvs = revalue(lvs, c('small'= 'seq/S'))
    lvs = revalue(lvs, c('seq'= 'seq/S'))

    levels(d$pattern) = lvs

    d$pattern = factor(d$pattern, levels=c('seq/S', 'rand/L', 'mix'))
    return(d)
}


order_appname_levels <- function(d)
{
    d = transform(d, appname = 
          factor(appname, levels=c('leveldb', 'rocksdb', 'sqliteRB', 'sqliteWAL', 'varmail')))

    return(d)
}


#### To use:
#### in herite this class and override get_raw_data(), 
#### then you can get_data() automatically support caching.
PlotWithCache <- setRefClass("PlotWithCache",
    methods = list(
        get_raw_data = function(exp_rel_path)
        {
            stop('implement this by yourself.')
        },
        get_data = function(exp_rel_path, classname, try_cache=TRUE)
        {
            my_cache_path = cache_path(exp_rel_path, classname)
            print(paste('Cache path:', my_cache_path))
            if (try_cache && cache_exists(exp_rel_path, classname))  {
                print('Using cache')
                print(my_cache_path)
                load(my_cache_path)
            } else {
                print('No cache')
                d = get_raw_data(exp_rel_path)
                print(my_cache_path)
                save(d, file=my_cache_path)
            }
           return(d)
        },
        cache_path = function(exp_rel_path, classname)
        {
            exp_rel_path = gsub('/', '', exp_rel_path)
            exp_rel_path = paste(exp_rel_path, collapse='-')
            cache_path = paste(digest(exp_rel_path), classname, 'rdata', sep='.')

            return(cache_path)
        },
        cache_exists = function(exp_rel_path, classname)
        {
            path = cache_path(exp_rel_path, classname)
            return( file.exists(path) )
        }
    )
)

































