# inspired by code from https://github.com/tensorflow/examples/blob/master/courses/udacity_intro_to_tensorflow_for_deep_learning/l08c01_common_patterns.ipynb

from collections import deque
import math, random
import matplotlib.pyplot as plt

def trend(time, slope=0):
    return slope * time


def seasonal_pattern(season_time):
    """Just an arbitrary pattern, you can change it if you wish"""
    if season_time < 0.4:
        return  math.cos(season_time * 2 * math.pi)
    else:
        return 1 / math.exp(3 * season_time)

def seasonality(time, period, amplitude=1, phase=0):
    """Repeats the same pattern at each period"""
    season_time = ((time + phase) % period) / period
    return amplitude * seasonal_pattern(season_time)

def white_noise(time, noise_level=1, seed=None):
    random.seed(seed)
    return random.normalvariate(0,1) * noise_level

def sensor(baseline=0, slope=0, period = 20, amplitude=20, phase=0, noise_level=1, start=0, end=-1):
    time = start
    while(time<end or end<0):
        yield baseline + trend(time, slope) + seasonality(time, period, amplitude, phase) + white_noise(time, noise_level)
        time += 1

if __name__ == '__main__':
    data_buffer = deque()
    plt.ion()
    plt.show()
    fig, ax = plt.subplots() #fig, (ax1, ax2) = plt.subplots(2, sharex=True)
    #ax.plot(range(5))
    #fig = plt.figure()
    #ax = fig.add_subplot(111)
    for e in sensor(baseline=10, slope=0.1, period = 100, amplitude= 40, noise_level=5, end=1000):
        data_buffer.append(e)
        ax.clear()
        ax.plot(data_buffer)
        #plt.plot(data_buffer)
        #plt.draw()
        plt.pause(0.01)

    # pause execution so you can examin the figure
    input("Press Enter to continue...")
