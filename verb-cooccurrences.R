dataset <- read_ods("annotated/thats-conjunctions.ods")
without_complement <- subset(dataset, !grepl("so.", verb))
with_complement <- subset(dataset, grepl("so.", verb))

write.table(as.data.frame(txt_freq(dataset$verb)), "annotated/combined.txt", sep = "\t", quote = FALSE, row.names = FALSE)
write.table(as.data.frame(txt_freq(without_complement$verb)), "annotated/without_complement.txt", sep = "\t", quote = FALSE, row.names = FALSE)
write.table(as.data.frame(txt_freq(with_complement$verb)), "annotated/with_complement.txt", sep = "\t", quote = FALSE, row.names = FALSE)

dataset_zeros <- read_ods("annotated/zeros.ods")
write.table(as.data.frame(txt_freq(dataset_zeros$verb)), "annotated/verbs-zeros.txt", sep = "\t", quote = FALSE, row.names = FALSE)