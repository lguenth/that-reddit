library(udpipe)
library(dplyr)
library(readODS)

thats <- udpipe_read_conllu("filtered/thats-conjunctions-aita-tifu-combined.conllu")

filtered_df <- thats %>%
  filter(grepl("that", token) & grepl("SCONJ", upos))

merged_df <- thats %>%
  group_by(sentence_id) %>%
  summarize_all(list(~paste(., collapse = " ")))

filtered_merged_df <- semi_join(merged_df, filtered_df, by = "sentence_id")

filtered_merged_df <- filtered_merged_df %>%
  select(-token_id, -doc_id, -paragraph_id, -head_token_id, -deps)

write.csv(filtered_merged_df, "data.csv", row.names = FALSE)