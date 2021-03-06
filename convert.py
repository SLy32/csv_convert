import os
import sys
import csv
import pandas as pd
import time


class Profiler(object):
    def __enter__(self):
        self._startTime = time.time()

    def __exit__(self, type, value, traceback):
        print("Elapsed time: {:.3f} sec".format(time.time() - self._startTime))


class Convert:
    """ For data from https://docs.google.com/spreadsheets/d/1kvPoupSzsSFBNSztMzl04xMoSC3Kcx3CrjVf4yBmESU/edit?ts=5b5f17db#gid=227859291 """
    def __init__(self, name):
        self.path_to = f"{os.path.dirname(os.path.abspath(__file__))}"
        self.input_file = f"{self.path_to}/input_data/{name}"
        self.output_file = f'{self.path_to}/output_data/{name}'
        path = name.split('/')
        print(path)
        if len(path) > 0:
            full_path = f"{self.path_to}/output_data"
            for item in list(path)[:-1]:
                full_path += f"/{item}"
                print(full_path)
                if not os.path.isdir(full_path):
                    os.mkdir(full_path)

    def lazy(self):
        with open(self.input_file, newline='') as f:
            r = csv.reader(f)
            for row in r:
                yield row

    def run(self):
        with Profiler() as p:
            row = self.lazy()
            with open(f"{self.output_file}", "w", newline='') as output_csv:
                writer = csv.writer(output_csv, delimiter='\t')

                for count, item in enumerate(row):
                    if count > 0:
                        data = item[0].split('\t')
                        dt = data[0].split(':')
                        data.insert(0, dt[0])
                        data.insert(1, dt[1])
                        data.insert(2, dt[2])
                        data.insert(3, dt[3])
                        writer.writerow(data)
                        if count > 5:
                            break
                    else:
                        header = item[0].split('\t')
                        header.insert(0, 'chrom')
                        header.insert(1, 'pos')
                        header.insert(2, 'ref')
                        header.insert(3, 'alt')

                        print(header)
                        i = header.index('minor_AF')
                        header.pop(i)
                        header.insert(i, 'MAF')

                        print(header)
                        writer.writerow(header)

    def only_header(self):
        with Profiler() as p:
            # row = self.lazy()
            with open(f"{self.input_file}", "w", newline='') as output_csv:
                writer = csv.writer(output_csv, delimiter='\t')
                for count, item in enumerate(row):
                    print(count, item)
                    if count < 1:
                        print(item[0])
                        res = item[0].replace('minor_AF', 'MAF')
                        res = res.replace('p_hwe', 'pval')
                        print(res)
                        writer.writerow(res)
                    else:
                        writer.writerow(item[0])
                        print(item)
                    if count > 1:
                        break

    def short(self):
        with Profiler() as p:
            row = self.lazy()
            with open(f"{self.output_file}", "w", newline='') as output_csv:
                writer = csv.writer(output_csv, delimiter='\t')

                for count, item in enumerate(row):
                    if count > 0:
                        data = item[0].split('\t')
                        dtw = []
                        dt = data[0].split(':')
                        dtw.insert(0, dt[0])
                        dtw.insert(1, dt[1])
                        dtw.insert(2, dt[2])
                        dtw.insert(3, dt[3])
                        dtw.insert(4, data[2])
                        dtw.insert(5, data[11])
                        writer.writerow(dtw)
                        if count > 2000:
                            break
                    else:
                        header = []
                        header.insert(0, 'chrom')
                        header.insert(1, 'pos')
                        header.insert(2, 'ref')
                        header.insert(3, 'alt')
                        header.insert(4, 'MAF')
                        header.insert(5, 'pval')
                        print(header)
                        print(len(item[0].split('\t')))
                        writer.writerow(header)

    def full_copy(self):
        with Profiler() as p:
            row = self.lazy()
            with open(f"{self.output_file}", "w", newline='') as output_csv:
                writer = csv.writer(output_csv, delimiter='\t')
                xcount = 0
                for count, item in enumerate(row):
                    # print(f"Row: {count}")
                    if count == 0:
                        data = item[0].split('\t')
                        # print(data)
                        header = []
                        header.insert(0, 'chrom')
                        header.insert(1, 'pos')
                        header.insert(2, 'ref')
                        header.insert(3, 'alt')
                        header.extend(data)
                        # print(header)
                        for i, item in enumerate(header):
                            if item == 'minor_AF':
                                header.pop(i)
                                header.insert(i, 'MAF')
                            if item == 'p_hwe':
                                header.pop(i)
                                header.insert(i, 'pval')
                        # print(header)
                        writer.writerow(header)
                        # print(len(header))
                    else:
                        data = item[0].split('\t')
                        # print(len(data))
                        # print(data)
                        dtw = []
                        dt = data[0].split(':')
                        dtw.insert(0, dt[0])
                        dtw.insert(1, dt[1])
                        dtw.insert(2, dt[2])
                        dtw.insert(3, dt[3])
                        dtw.extend(data)
                        writer.writerow(dtw)
                        # print(len(dtw))
                    xcount += 1
                    if xcount > 1000000:
                        print(count)
                        xcount = 0
                        # break
            print(f"File size - {round(os.stat(self.output_file).st_size/1024/1024, 2)} Kb")

    def panda_conv(self):
        with Profiler() as p:
            df = pd.read_csv(self.input_file, header=None, sep='\t')
            # print(df)
            for index, row in df.iterrows():
                print(index, row)
                if index > 10:
                    break


if __name__ == "__main__":
    path_to = f"{os.path.dirname(os.path.abspath(__file__))}"
    file = '100007_irnt.gwas.imputed_v3.both_sexes.tsv/100007_irnt.gwas.imputed_v3.both_sexes.tsv'
    convert = Convert(file)
    print(path_to, file)
    # convert.full_copy()
    convert.panda_conv()
