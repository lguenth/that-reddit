# %% [markdown]
# ## Functions

# %%
import os
import json
import csv
import zstandard as zstd

path = "../raw/"

posts = [file for file in os.scandir(path) if '_submissions' in file.name and file.is_file()]
# comments = [file for file in os.scandir(path) if 'AmItheAsshole_comments' in file.name and file.is_file()]

# %%
def read_and_decode(
    reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0
):
    chunk = reader.read(chunk_size)
    bytes_read += chunk_size
    if previous_chunk is not None:
        chunk = previous_chunk + chunk
    try:
        return chunk.decode()
    except UnicodeDecodeError:
        if bytes_read > max_window_size:
            raise UnicodeError(
                f"Unable to decode frame after reading {bytes_read:,} bytes"
            )

        print(f"Decoding error with {bytes_read:,} bytes, reading another chunk")

        return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(path):
    with open(path, "rb") as file:
        buffer = ""
        reader = zstd.ZstdDecompressor(max_window_size=2**31).stream_reader(file)

        while True:
            chunk = read_and_decode(reader, 2**27, (2**29) * 2)
            if not chunk:
                break

            lines = (buffer + chunk).split("\n")

            for line in lines[:-1]:
                yield line.strip(), file.tell()

            buffer = lines[-1]

        reader.close()


def json_to_csv(outputs, output_path, fields):
    with open(output_path, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fields)
        writer.writeheader()

        for json_object in outputs:
            row = {}
            for field in fields:
                if field in json_object:
                    row[field] = json_object[field]
                else:
                    row[field] = ""
            writer.writerow(row)

# %% [markdown]
# ## Submissions

# %%
for post in posts:
    input_path = post.path
    sub = post.name.replace('.zst', '')
    output_path = f"../filtered/{sub}-filtered.csv"

    fields = [
        "id",
        "author",
        "created_utc",
        # "edited",
        # "hidden",
        "num_comments",
        "num_crossposts",
        "permalink",
        "over_18",
        "score",
        "selftext",
        "subreddit",
        "subreddit_id",
        "title",
        "url",
    ]

    total_lines = 0
    outputs = []

    for line, bytes_processed in read_lines_zst(input_path):
        total_lines += 1

        try:
            data = json.loads(line)

            if not data["over_18"] and data["score"] >= 1000 and data["num_comments"] >= 10 and not any(value in data["selftext"] for value in ["[deleted]", "[archived]", "[removed]"]):
                outputs.append(data)

        except Exception as err:
            print(err)

    json_to_csv(outputs, output_path, fields)

    with open(f"../pickled/{sub}-filtered.pkl", "wb") as pf:
        pk.dump(outputs, pf, pk.HIGHEST_PROTOCOL)

    print(f"File: {post.name}")
    print(f"Total lines processed: {total_lines}")
    print(f"Total lines after filtering: {len(outputs)}")

# %% [markdown]
# ### Dataframes

# %%
import pandas as pd
import pickle as pk

for file in os.scandir("../pickled/"):
    if "-that" in file.name or "-no_that" in file.name:
        os.remove(file)

pickled_subs = [file for file in os.scandir("../pickled/") if file.name.endswith('_submissions-filtered.pkl') and file.is_file()]

for sub in pickled_subs:
    with open(sub.path, "rb") as f:
        outputs = pk.load(f)

    df = pd.DataFrame(outputs, columns=fields).sort_values(by="num_comments", ascending=False, ignore_index=True)

    if "amitheasshole" in sub.name.lower():
        df = df.loc[~df.author.str.contains("AITAMod")]
    elif "askscience" in sub.name.lower():
        df = df.loc[~df.author.str.contains("AskScienceModerator")]
    elif "askhistorians" in sub.name.lower():
        df = df.loc[~df.author.str.contains("crrpit")].loc[~df.author.str.contains("eternalkerri")]

    df.created_utc = pd.to_datetime(df.created_utc, unit="s", errors="ignore")

    that = df.loc[df.selftext.str.contains("that")].copy().reset_index(drop=True)
    no_that = df.loc[~df.selftext.str.contains("that")].copy().reset_index(drop=True)

    with open(f"../pickled/{sub.name.replace('.pkl', '')}-that.pkl", "wb") as pf:
        pk.dump(that, pf, pk.HIGHEST_PROTOCOL)

    with open(f"../pickled/{sub.name.replace('.pkl', '')}-no_that.pkl", "wb") as pf:
        pk.dump(no_that, pf, pk.HIGHEST_PROTOCOL)

# %% [markdown]
# ### 

# %% [markdown]
# ### Top 100 - that

# %%
import shutil

shutil.rmtree('../that-submissions')

# %%
os.makedirs("../that-submissions/aita", exist_ok=True)

with open("../pickled/AmItheAsshole_submissions-filtered-that.pkl", "rb") as f:
    that = pk.load(f)

line_count = 0
that_count = 0
entries = that.iloc[:100].copy()

for index, item in entries.iterrows():
    lines = [sent.strip() for sent in item.selftext.split("\n") if sent.strip()]
    thats = sum([line.count('that') for line in lines])
    
    line_count += len(lines)
    that_count += thats

    if lines:
        with open(f"../that-submissions/aita/{item.id}.txt", "w+") as f:
            f.write("\n".join(lines))

print(f"Average number of lines: {line_count/len(entries):.2f}")
print(f"Average number of thats: {that_count/len(entries):.2f}")

# %%
os.makedirs("../that-submissions/askhist", exist_ok=True)

with open("../pickled/AskHistorians_submissions-filtered-that.pkl", "rb") as f:
    that = pk.load(f)

line_count = 0
that_count = 0
entries = that.iloc[:100].copy()

for index, item in entries.iterrows():
    lines = [sent.strip() for sent in item.selftext.split("\n") if sent.strip()]
    thats = sum([line.count('that') for line in lines])
    
    line_count += len(lines)
    that_count += thats

    if lines:
        with open(f"../that-submissions/askhist/{item.id}.txt", "w+") as f:
            f.write("\n".join(lines))

print(f"Average number of lines: {line_count/len(entries):.2f}")
print(f"Average number of thats: {that_count/len(entries):.2f}")

# %%
os.makedirs("../that-submissions/asksci", exist_ok=True)

with open("../pickled/askscience_submissions-filtered-that.pkl", "rb") as f:
    that = pk.load(f)

line_count = 0
that_count = 0
entries = that.iloc[:100].copy()

for index, item in entries.iterrows():
    lines = [sent.strip() for sent in item.selftext.split("\n") if sent.strip()]
    thats = sum([line.count('that') for line in lines])
    
    line_count += len(lines)
    that_count += thats

    if lines:
        with open(f"../that-submissions/asksci/{item.id}.txt", "w+") as f:
            f.write("\n".join(lines))

print(f"Average number of lines: {line_count/len(entries):.2f}")
print(f"Average number of thats: {that_count/len(entries):.2f}")

# %%
os.makedirs("../that-submissions/tifu", exist_ok=True)

with open("../pickled/tifu_submissions-filtered-that.pkl", "rb") as f:
    that = pk.load(f)

line_count = 0
that_count = 0
entries = that.iloc[:100].copy()

for index, item in entries.iterrows():
    lines = [sent.strip() for sent in item.selftext.split("\n") if sent.strip()]
    thats = sum([line.count('that') for line in lines])
    
    line_count += len(lines)
    that_count += thats

    if lines:
        with open(f"../that-submissions/tifu/{item.id}.txt", "w+") as f:
            # print(f"{item.id} - {len(lines)} - {thats}")
            f.write("\n".join(lines))

print(f"Average number of lines: {line_count/len(entries):.2f}")
print(f"Average number of thats: {that_count/len(entries):.2f}")

# %% [markdown]
# ### Top 100 - non-that

# %%
import shutil

shutil.rmtree('../no_that-submissions')

# %%
os.makedirs("../no_that-submissions/aita", exist_ok=True)

with open("../pickled/AmItheAsshole_submissions-filtered-no_that.pkl", "rb") as f:
    no_that = pk.load(f)

line_count = 0
that_count = 0
entries = no_that.iloc[:100].copy()

for index, item in entries.iterrows():
    lines = [sent.strip() for sent in item.selftext.split("\n") if sent.strip()]
    
    line_count += len(lines)

    if lines:
        with open(f"../no_that-submissions/aita/{item.id}.txt", "w+") as f:
            f.write("\n".join(lines))

print(f"Average number of lines: {line_count/len(entries):.2f}")

# %%
os.makedirs("../no_that-submissions/asksci", exist_ok=True)

with open("../pickled/askscience_submissions-filtered-no_that.pkl", "rb") as f:
    no_that = pk.load(f)

line_count = 0
that_count = 0
entries = no_that.iloc[:100].copy()

for index, item in entries.iterrows():
    lines = [sent.strip() for sent in item.selftext.split("\n") if sent.strip()]
    
    line_count += len(lines)

    if lines:
        with open(f"../no_that-submissions/asksci/{item.id}.txt", "w+") as f:
            f.write("\n".join(lines))

print(f"Average number of lines: {line_count/len(entries):.2f}")

# %%
os.makedirs("../no_that-submissions/askhist", exist_ok=True)

with open("../pickled/AskHistorians_submissions-filtered-no_that.pkl", "rb") as f:
    no_that = pk.load(f)

line_count = 0
that_count = 0
entries = no_that.iloc[:100].copy()

for index, item in entries.iterrows():
    lines = [sent.strip() for sent in item.selftext.split("\n") if sent.strip()]
    
    line_count += len(lines)

    if lines:
        with open(f"../no_that-submissions/askhist/{item.id}.txt", "w+") as f:
            f.write("\n".join(lines))

print(f"Average number of lines: {line_count/len(entries):.2f}")

# %%
os.makedirs("../no_that-submissions/tifu", exist_ok=True)

with open("../pickled/tifu_submissions-filtered-no_that.pkl", "rb") as f:
    no_that = pk.load(f)

line_count = 0
that_count = 0
entries = no_that.iloc[:100].copy()

for index, item in entries.iterrows():
    lines = [sent.strip() for sent in item.selftext.split("\n") if sent.strip()]
    
    line_count += len(lines)

    if lines:
        with open(f"../no_that-submissions/tifu/{item.id}.txt", "w+") as f:
            f.write("\n".join(lines))

print(f"Average number of lines: {line_count/len(entries):.2f}")

# %%
pickled_subs = [file for file in os.scandir("../pickled/") if file.name.endswith('filtered-that.pkl') and file.is_file()]

for sub in pickled_subs:
    with open(sub.path, "rb") as f:
        that = pk.load(f)

    ids = list(set(that.id))

    with open(f"../metadata/{sub.name.replace('.pkl', '')}-ids.txt", "w+") as f:
        f.write("\n".join(ids))

    for uid in ids:
        coms = []
        submission = reddit.submission(uid)
        
        submission.comments.replace_more(limit=0)

        for top_level_comment in submission.comments:
            if top_level_comment.score > 1000:
                coms.append(top_level_comment.body)

    with open(f"../comments/{sub.name.replace('.pkl', '')}-{uid}-comments.txt", "w+") as f:
        f.write("\n".join(coms))

# %%
input_path = "../raw/AmItheAsshole_comments.zst"
output_path = f"../filtered/AmItheAsshole_comments-filtered.csv"

fields = [
    "id",
    "author",
    "body",
    "edited",
    "score",
    "parent_id",
    "link_id",
    "created_utc",
    "subreddit",
    "subreddit_id",
]

parent_ids = list(set(that.id))
total_lines = 0
comment_outputs = []

for line, bytes_processed in read_lines_zst(input_path):
    total_lines += 1

    try:
        data = json.loads(line)

        if data["parent_id"] in parent_ids and data["score"] > 100:
            comment_outputs.append(data)

    except Exception as err:
        print(err)

json_to_csv(comment_outputs, output_path, fields)

print(f"Total lines processed: {total_lines}")
print(f"Total lines after filtering: {len(comment_outputs)}")

# %% [markdown]
# ## Metadata

# %%
# Take a look at the data structures
sub_path = posts[0].path
sub_size = posts[0].stat().st_size
sub_lines = read_lines_zst(sub_path)
sub_line = next(sub_lines)[0]
sub_data = json.loads(sub_line)

print(f"File size, submissions: {sub_size}")
with open('../metadata/submissions-struct.json', 'w', encoding='utf-8') as f:
    json.dump(sub_data, f, ensure_ascii=False, indent=4)

com_path = comments[0].path
com_size = comments[0].stat().st_size
com_lines = read_lines_zst(com_path)
com_line = next(com_lines)[0]
com_data = json.loads(com_line)

print(f"File size, comments: {com_size}")
with open('../metadata/comments-struct.json', 'w', encoding='utf-8') as f:
    json.dump(com_data, f, ensure_ascii=False, indent=4)


