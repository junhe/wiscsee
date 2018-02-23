# libraries
library(ggplot2)
library(plyr)
library(dplyr)
library(reshape2)
library(gridExtra)
library(jsonlite)
library(digest)
library(splitstackshape)
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
    WORKDIRECTORY= "/Users/junhe/workdir/zombie-curve/"
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

plot <- function() {
    json_data = read_json("recorder.json")
    snap_shots = json_data[['ftl_func_valid_ratios']]
    d = organize_snapshots(snap_shots)

    print(head(d))

    # d = subset(d, snapshot_id < 10)
    # plot_moving_snake(d)
    plot_zombie_curves(d)
}

plot_moving_snake <- function(d)
{
    d = ddply(d, .(snapshot_id), set_moving_snake_points_for_a_snapshot)
    d = transform(d, 
      block_location = (as.numeric(blocknum)/GB) * as.numeric(ERASE_BLOCK_SIZE))

    p = ggplot(d, aes(frame=snapshot_id)) +
        geom_ribbon(aes(x=block_location, ymin=0, ymax=1), fill='grey') +
        geom_ribbon(aes(x=block_location, ymin=0, ymax=valid_ratio), fill='black') +
        ylab('Valid Ratio') +
        xlab('Accumulated Block Size (GB)') 
    gganimate(p, paste('moving-snake777', 'gif', sep='.'), interval=0.1)
}

plot_zombie_curves <- function(d)
{
    d = subset(d, valid_ratio > 0)
    d = ddply(d, .(snapshot_id), set_snake_points_for_a_snapshot)
    d = transform(d, 
      block_location = (as.numeric(blocknum)/GB) * as.numeric(ERASE_BLOCK_SIZE))

    p = ggplot(d, aes(frame=snapshot_id)) +
        geom_line(aes(x=block_location, y=valid_ratio), size=3) +
        ylab('Valid Ratio') +
        xlab('Accumulated Block Size (GB)') 
    gganimate(p, paste('moving-snake777', 'gif', sep='.'), interval=0.1)
}


main <- function()
{
    plot()
}

main()

