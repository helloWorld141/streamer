while (TRUE){
    value <- read.delim('out', header = FALSE, sep = ",")
    print(value)
    Sys.sleep(1)
}