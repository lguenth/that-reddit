library("ggplot2")
library("dplyr")
library("udpipe")

files <- list.files(path = "automatic-annotation/aita/", pattern="*.conllu", full.names = FALSE)
paths <- list.files(path = "automatic-annotation/aita/", pattern="*.conllu", full.names = TRUE)

aita_df <- ldply(1:length(paths), function(i) {
  cl <-  udpipe_read_conllu(paths[i])
  cl$file <- gsub(".conllu", "", files[i])
  cl
})

aita_df <- subset(aita_df, select= !(names(aita_df) %in% c("doc_id", "deps")))

aita_that <- subset(aita_df, token %in% c("that"))
stats_aita_that <- txt_freq(aita_that$upos)
stats_aita_that$key <- factor(stats_aita_that$key, levels = stats_aita_that$key)

ggplot(stats_aita_that, aes(x = key, y = freq)) +
  geom_bar(stat = "identity", fill = "darkblue") +
  geom_text(aes(label = freq), vjust = -0.5, size = 3.5) +
  labs(x = "Part of speech",
       y = "Frequency")

tifu_files <- list.files(path = "automatic-annotation/tifu/", pattern="*.conllu", full.names = FALSE)
tifu_paths <- list.files(path = "automatic-annotation/tifu/", pattern="*.conllu", full.names = TRUE)

tifu_df <- ldply(1:length(tifu_paths), function(i) {
  cl <-  udpipe_read_conllu(tifu_paths[i])
  cl$file <- gsub(".conllu", "", tifu_files[i])
  cl
})

tifu_df <- subset(tifu_df, select= !(names(tifu_df) %in% c("doc_id", "deps")))

tifu_that <- subset(tifu_df, token %in% c("that"))
stats_tifu_that <- txt_freq(tifu_that$upos)
stats_tifu_that$key <- factor(stats_tifu_that$key, levels = stats_tifu_that$key)

ggplot(stats_tifu_that, aes(x = key, y = freq)) +
  geom_bar(stat = "identity", fill = "darkblue") +
  geom_text(aes(label = freq), vjust = -0.5, size = 3.5) +
  labs(x = "Part of speech",
       y = "Frequency")

merged_df <- rbind(aita_that, tifu_that)
stats_combined_that <-txt_freq(merged_df$upos)
stats_combined_that$key <- factor(stats_combined_that$key, levels = stats_combined_that$key)

ggplot(stats_combined_that, aes(x = key, y = freq)) +
  geom_bar(stat = "identity", fill = "darkblue") +
  geom_text(aes(label = freq), vjust = -0.5, size = 3.5) +
  labs(x = "Part of speech",
       y = "Frequency")

### Manually annotated data (combined)

manual_data <- data.frame(
  key = c("SCONJ", "PRON", "DET", "ADV"),
  freq = c(452, 263, 56, 14)
)

manual_data$key <- factor(manual_data$key, levels = manual_data$key)

ggplot(manual_data, aes(x = key, y = freq)) +
  geom_bar(stat = "identity", fill = "darkblue") +
  geom_text(aes(label = freq), vjust = -0.5, size = 3.5) +
  labs(x = "Part of speech",
       y = "Frequency")

manual_data$group <- "C1"
stats_combined_that_new <- subset(stats_combined_that, select = -freq_pct)
stats_combined_that_new$group <- "C2"
combined_df <- rbind(stats_combined_that_new, manual_data)

ggplot(combined_df, aes(x = key, y = freq, fill = group)) +
  geom_bar(stat = "identity", position = "dodge") +
  geom_text(aes(label = freq), position = position_dodge(width = .95), vjust = -0.5, size = 3.5) +
  labs(x = "Part of speech", y = "Frequency", fill = "Corpus")