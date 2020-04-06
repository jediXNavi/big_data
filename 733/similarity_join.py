import re
import pandas as pd
import numpy as np

class SimilarityJoin:
    def __init__(self, data_file1, data_file2):
        self.df1 = pd.read_csv(data_file1)
        self.df2 = pd.read_csv(data_file2)

    def preprocess_df(self, df, cols):
        df['joinkey'] = df[cols[0]].replace(np.nan,'') + ' ' + df[cols[1]].replace(np.nan,'')
        df['joinkey'] = df['joinkey'].map(lambda x: re.split(r'\W+',str(x)))
        for i in df['joinkey']:
            while("" in i):
                i.remove("")
        df = df[['id','joinkey']]

        return df

    def filtering(self, df1, df2):
        df1_new = df1.explode('joinkey')
        df2_new = df2.explode('joinkey')
        join_df = df1_new.merge(df2_new,left_on='joinkey',right_on='joinkey',suffixes=('1','2')).drop(columns=['joinkey'])
        join_df = join_df.drop_duplicates(subset=['id1','id2'])
        join_df1_id = join_df.merge(df1,left_on='id1',right_on='id').rename(columns={"joinkey":"joinkey1"})
        cand_df = join_df1_id.merge(df2,left_on='id2',right_on='id').rename(columns={"joinkey":"joinkey2"}).drop(columns=['id_x','id_y'])

        return cand_df

    def verification(self, cand_df, threshold):
        cand_df['jaccard_intersection'] = [list(set(a).intersection(set(b))) for a, b in zip(cand_df['joinkey1'], cand_df['joinkey2'])]
        cand_df['jaccard_intersection'] = cand_df['jaccard_intersection'].map(lambda x: len(x))
        cand_df['jaccard_union'] = [list(set(a).union(set(b))) for a, b in zip(cand_df['joinkey1'], cand_df['joinkey2'])]
        cand_df['jaccard_union'] = cand_df['jaccard_union'].map(lambda x: len(x))
        cand_df['jaccard_index'] = cand_df['jaccard_intersection']/cand_df['jaccard_union']
        output_df = cand_df[cand_df['jaccard_index'] >= threshold]

        return output_df


    def evaluate(self, result, ground_truth):
        result = set(tuple(x) for x in result)
        ground_truth = set(tuple(x) for x in ground_truth)
        T = len(list(set(result).intersection(ground_truth)))
        R = len(result)
        A = len(ground_truth)
        precision = T/R
        recall = T/A
        fmeasure = (2*precision*recall)/(precision+recall)

        return (precision, recall, fmeasure)

    def jaccard_join(self, cols1, cols2, threshold):
        new_df1 = self.preprocess_df(self.df1, cols1)
        new_df2 = self.preprocess_df(self.df2, cols2)
        print("Before filtering: %d pairs in total" % (self.df1.shape[0] * self.df2.shape[0]))
        cand_df = self.filtering(new_df1, new_df2)
        print("After Filtering: %d pairs left" % (cand_df.shape[0]))

        result_df = self.verification(cand_df, threshold)
        print("After Verification: %d similar pairs" % (result_df.shape[0]))

        return result_df


if __name__ == "__main__":
    er = SimilarityJoin("Amazon.csv", "Google.csv")
    amazon_cols = ["title", "manufacturer"]
    google_cols = ["name", "manufacturer"]
    result_df = er.jaccard_join(amazon_cols, google_cols, 0.5)

    result = result_df[['id1', 'id2']].values.tolist()
    ground_truth = pd.read_csv("Amazon_Google_perfectMapping.csv").values.tolist()
    print("(precision, recall, fmeasure) = ", er.evaluate(result, ground_truth))