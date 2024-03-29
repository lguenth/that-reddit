import glob
import pathlib
import pickle as pk
from cassis import *
import pandas as pd

SENTENCE_TYPE = "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence"
TOKEN_TYPE = "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token"
meta = []


def process_file(path, project_id):
    with open(path, "rb") as f:
        cas = load_cas_from_json(f)
        text = cas.sofa_string
        uid = cas.get_document_annotation().documentTitle.rstrip(".txt")
        sentences = cas.select(SENTENCE_TYPE)

        # annotation_types = [t for t in cas.typesystem.get_types()]
        # print(annotation_types)
        thats, zeros, tokens = [], [], []

        for sentence in sentences:
            xmi_id = sentence.get("xmiID")
            sent_begin = sentence.begin
            sent_end = sentence.end

            # that occurrences & Part of speech
            for token in cas.select_covered(TOKEN_TYPE, sentence):
                tokens.append(token)
                pos = token.pos
                if pos:
                    upos = pos.get("coarseValue")
                    xpos = pos.get("PosValue")
                    that_begin = token.begin
                    that_end = token.end

                    if upos:
                        thats.append(
                            {
                                "sentence": sentence.get_covered_text(),
                                "sentence_id": xmi_id,
                                "sentence_begin": sent_begin,
                                "sentence_end": sent_end,
                                # "isZero": False,
                                "upos": upos,
                                "xpos": xpos,
                                "begin": that_begin,
                                "end": that_end,
                            }
                        )

            # that-zero occurrences
            try:
                for zero in cas.select_covered("custom.Span", sentence):
                    if zero.IsZero == True:
                        zero_text = zero.get_covered_text()
                        zero_begin = zero.begin
                        zero_end = zero.end

                        zeros.append(
                            {
                                "sentence": sentence.get_covered_text(),
                                "sentence_id": xmi_id,
                                "sentence_begin": sent_begin,
                                "sentence_end": sent_end,
                                # "isZero": True,
                                "annotated": zero_text,
                                "begin": zero_begin,
                                "end": zero_end,
                            }
                        )
            except:
                pass

    df_thats = pd.DataFrame(thats)
    df_zeros = pd.DataFrame(zeros)

    project_pk_path = f"pickled/annotations/{project_id}"
    pathlib.Path(project_pk_path).mkdir(parents=True, exist_ok=True)

    with open(f"{project_pk_path}/{uid}-thats.pkl", "wb") as fh:
        pk.dump(df_thats, fh, protocol=pk.HIGHEST_PROTOCOL)

    with open(f"{project_pk_path}/{uid}-zeros.pkl", "wb") as fh:
        pk.dump(df_zeros, fh, protocol=pk.HIGHEST_PROTOCOL)

    project_meta_path = f"metadata/annotations/{project_id}"
    pathlib.Path(project_meta_path + "/thats").mkdir(parents=True, exist_ok=True)
    pathlib.Path(project_meta_path + "/zeros").mkdir(parents=True, exist_ok=True)

    df_thats.to_csv(f"{project_meta_path}/thats/{uid}.csv", index=False)
    df_zeros.to_csv(f"{project_meta_path}/zeros/{uid}.csv", index=False)

    return uid, sentences, tokens, thats, zeros


if __name__ == "__main__":
    files = glob.glob("annotated/**/**/admin.json")

    for file in files:
        project_id = file.split("/")[1]
        file_id, sentences, tokens, thats, zeros = process_file(file, project_id)

        meta.append(
            {
                "project": project_id,
                "id": file_id,
                "count_sentences": len(sentences),
                "count_tokens": len(tokens),
                "count_that": len(thats),
                "count_zeros": len(zeros),
            }
        )

    df_meta = pd.DataFrame(meta)

    with open("pickled/annotations/submissions-basic_stats.pkl", "wb") as fh:
        pk.dump(df_meta, fh, protocol=pk.HIGHEST_PROTOCOL)
