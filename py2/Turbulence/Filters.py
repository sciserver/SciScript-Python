#Python v3.4

import numpy as np
from scipy.ndimage.filters import correlate1d
from scipy.ndimage.filters import gaussian_filter1d
import math

class BoxFilter:
    def __init__(self, filterHalf = 4):
        self.filterHalf = filterHalf
        self.border = filterHalf
    def filter(self, field3D):
        weights = np.ones((2*self.filterHalf + 1), dtype=np.float32) / (2*self.filterHalf + 1)
        result = correlate1d(field3D, weights, axis = 0)
        result = correlate1d(result, weights, axis = 1)
        return correlate1d(result, weights, axis = 2)
 
#I need to check and make dure that this filter doesn't make three passes over the pressure data.
class GaussianFilter:
    def __init__(self, sigma = 1):
        self.sigma = sigma
        if sigma > 1:
            self.border = int(math.ceil(sigma)) * 4
        else:
            self.border = 4
    def filter(self, field3D):
        #return field3D - 1
        result = gaussian_filter1d(field3D, self.sigma, axis = 0)
        result = gaussian_filter1d(result, self.sigma, axis = 1)
        result = gaussian_filter1d(result, self.sigma, axis = 2)
        #print "Are they the same?", (result == field3D) 
        return result
    
class SpectralCutoffFilter:
    def __init__(self, cutoff = 1):
        self.cutoff = cutoff
        self.border = 0

    def filter(self, field3D):
        resultFFT = np.fft.fft(field3D, axis = 0)
        resultFFT = np.fft.fft(resultFFT, axis = 1)
        resultFFT = np.fft.fft(resultFFT, axis = 2)

        print resultFFT.shape

        for x in resultFFT.real:
            for y in x:
                for z in y:

                    if z[0] > self.cutoff:
                        z[0] = self.cutoff

        resultFFT = np.fft.ifft(resultFFT, axis = 0)
        resultFFT = np.fft.ifft(resultFFT, axis = 1)
        resultFFT = np.fft.ifft(resultFFT, axis = 2)

        return resultFFT