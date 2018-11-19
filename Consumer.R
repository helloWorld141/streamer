while (TRUE){
    value <- read.delim('../streamer/out', header = FALSE, sep = ",")
    print(value)
    Sys.sleep(1)
}
