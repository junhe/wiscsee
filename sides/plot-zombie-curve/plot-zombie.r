require("ggplot2")
require("jsonlite")
require("reshape2")
require("plyr")

GB = 2^30
blocksize = 2^20

load_data <- function(file_path) {
    json_data = fromJSON(txt=file_path)
    return(json_data[['ftl_func_valid_ratios']])
}

extract_one_snapshot <- function(snapshots) {
    # get only the last snapshot
    dd = tail(snapshots, 1)

    dd = melt(as.matrix(dd))
    names(dd) = c('snapshot_id', 'valid_ratio', 'count')
    dd = subset(dd, is.na(count) == FALSE)
    dd$snapshot_id = NULL

    return(dd)
}

organize_data <- function(d) {
    d = arrange(d, desc(valid_ratio))
    d = transform(d, seg_end = cumsum(count))
    d = transform(d, seg_start = seg_end - count)
    d = melt(d, 
         measure = c('seg_start', 'seg_end'), value.name = 'blocknum')
    d = arrange(d, desc(valid_ratio))

    d = transform(d, block_location = (as.numeric(blocknum)/GB) * as.numeric(blocksize))
    d = subset(d, valid_ratio != 0)

    return(d)
}

plot <- function(d) {
    p = ggplot(d, aes(x = block_location, y = valid_ratio)) +
        geom_line() +
        ylab('Valid Ratio') +
        xlab('Cumulative Block Size (GB)')
    print(p)

    ggsave("plot.pdf", plot = p, height = 4, width = 4)
}


main <- function() {
    print("Hello")

    snapshots = load_data("./recorder.json")
    d = extract_one_snapshot(snapshots)
    d = organize_data(d)
    plot(d)
}

main()

