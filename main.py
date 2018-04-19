import csv
import numpy as np
import time
import multiprocessing as mp


def getsum(frame):
    summ = 0
    exch = {}  # Dictionary which contains names and number of transactions for exchanges in frame
    for record in frame:
        summ += int(record[2])
        if exch.get(record[3]) is None:
            tmp = {record[3]: 0}
            exch.update(tmp)
            exch[record[3]] += int(record[2])
    return summ, frame[0][0], frame[len(frame) - 1][0], exch


if __name__ == '__main__':
    pool = mp.Pool()
    exchanges = {}  # Dictionary which contains names and number of transactions for all exchanges
    sec = np.timedelta64(1, 's')  # Size of frame (one second)
    data = []
    frames = []
    max = (0, 0, 0)
    m = 0  # Number of frames
    t1 = time.time()
    # Read data
    with open('TRD.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            data.append(row)
    # Split data to frames
    for i in range(len(data)):
        border = i
        frames.append([])
        while abs(np.datetime64('2018-02-01T' + data[i][0]) - np.datetime64('2018-02-01T' + data[border][0])) < sec:
            frames[m].append(data[border])
            if exchanges.get(data[border][3]) is None:
                tmp = {data[border][3]: (0, 0, 0)}
                exchanges.update(tmp)
            border += 1
            if border == len(data): break
        m += 1
    # Calculate number of transactions for each frame
    res = pool.map(getsum, frames)
    # Find frame with most transactions in one second
    for i in res:
        if int(i[0]) > max[0]:
            max = i
        for j in range(len(i[3])):
            tmp = i[3].popitem()
            if tmp[1] > exchanges.get(tmp[0])[0]:
                exchanges.update({tmp[0]: (tmp[1], i[1], i[2])})
    print('Most transactions(', max[0], ') was made between ', max[1], ' and ', max[2])
    print('Information about each exchange is presented below:')
    for i in range(len(exchanges)):
        record = exchanges.popitem()
        print(record[0], '  ', record[1][0], '  ', record[1][1], ' - ', record[1][2])
    t2 = time.time()
    print('Answer has been given in ', t2 - t1, ' seconds.')