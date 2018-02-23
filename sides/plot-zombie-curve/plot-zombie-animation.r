# libraries
library(ggplot2)
library(dplyr)
library(plyr)
library(reshape2)
library(jsonlite)
library(gganimate)

KB = 2^10
MB = 2^20
GB = 2^30
TB = 2^40

SEC = 10^9
MILISEC = 10^6
MICROSEC = 10^3
NANOSEC = 1

ERASE_BLOCK_SIZE = 128*KB

# copy the following so you can do sme()
# setwd(WORKDIRECTORY)
sme <- function()
{
    WORKDIRECTORY= "/Users/junhe/workdir/zombie-curve/wiscsee/sides/plot-zombie-curve/"
    THISFILE     ='plot-zombie-animation.r'
    setwd(WORKDIRECTORY)
    source(THISFILE)
}


organize_snapshots <- function(snapshots){
    d = melt(as.matrix(snapshots))
    d = as.data.frame(d)
    names(d) = c('snapshot_id', 'valid_ratio', 'count')
    d = subset(d, is.na(count) == FALSE)
    return(d)
}

set_moving_snake_points_for_a_snapshot <- function(d) {
    d = arrange(d, valid_ratio)
    d = transform(d, seg_end = cumsum(count))
    d = transform(d, seg_start = seg_end - count)
    d = melt(d, 
         measure = c('seg_start', 'seg_end'), value.name = 'blocknum')
    d = arrange(d, valid_ratio)
}

set_snake_points_for_a_snapshot <- function(d) {
    d = arrange(d, desc(valid_ratio))
    d = transform(d, seg_end = cumsum(count))
    d = transform(d, seg_start = seg_end - count)
    d = melt(d, 
         measure = c('seg_start', 'seg_end'), value.name = 'blocknum')
    d = arrange(d, desc(valid_ratio))
}

plot <- function(files, names) {
    n = length(files)
    d_list = list()
    for (i in seq(n)) {
        print(files[i])
        print(names[i])
        json_data = read_json(files[i])
        snap_shots = json_data[['ftl_func_valid_ratios']]
        d = organize_snapshots(snap_shots)
        d$name = names[i]
        d_list = append(d_list, list(d))
    }

    d = do.call("rbind", d_list)

    print(head(d))

    d = subset(d, snapshot_id < 100)
    plot_zombie_curves(d)
}

plot_zombie_curves <- function(d)
{
    d = subset(d, valid_ratio > 0)
    d = ddply(d, .(name, snapshot_id), set_snake_points_for_a_snapshot)
    d = transform(d, 
      block_location = (as.numeric(blocknum)/GB) * as.numeric(ERASE_BLOCK_SIZE))

    p = ggplot(d, aes(frame=snapshot_id)) +
        geom_line(aes(x=block_location, y=valid_ratio, color=name), size=1) +
        ylab('Valid Ratio') +
        xlab('Accumulated Block Size (GB)') 
    gganimate(p, paste('zombie-curve-animation', 'gif', sep='.'), interval=0.1)
}

# Black area shows valid data
# Gray area shows the invalid data
plot_zombie_area <- function(path)
{
    json_data = read_json(path)
    snap_shots = json_data[['ftl_func_valid_ratios']]
    d = organize_snapshots(snap_shots)

    d = ddply(d, .(snapshot_id), set_moving_snake_points_for_a_snapshot)
    d = transform(d, 
      block_location = (as.numeric(blocknum)/GB) * as.numeric(ERASE_BLOCK_SIZE))

    p = ggplot(d, aes(frame=snapshot_id)) +
        geom_ribbon(aes(x=block_location, ymin=0, ymax=1), fill='grey') +
        geom_ribbon(aes(x=block_location, ymin=0, ymax=valid_ratio), fill='black') +
        ylab('Valid Ratio') +
        xlab('Accumulated Block Size (GB)') 
    gganimate(p, paste('moving-snake', 'gif', sep='.'), interval=0.1)
}



main <- function()
{
    # plot(c("./recorder-ext4-varmail-small.json", 
           # "./recorder-f2fs-varmail-small.json"), 
         # c("Varmail-ext4", "Varmail-f2fs"))

    plot(c("./recorder-ext4-sqlite-rb-rand-nonsegmented.json", 
           "./recorder-f2fs-sqlite-rb-rand-nonsegmented.json"), 
         c("SQLite-ext4", "SQLite-f2fs"))
}

main()

