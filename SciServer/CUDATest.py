import os
os.environ["CUDA_DEVICE"] = "1"

import pycuda.autoinit
import pycuda.driver as drv
import numpy as np

import SciServer.CasJobs
import pandas
import tables

from pycuda.compiler import SourceModule

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

import pycuda.gpuarray as gpuarray

RadiusMax = 2

haloID = "313000080000000"

queryString = "SELECT p.* FROM (SELECT x,y,z FROM mpahalotrees..mr WHERE haloId=" + haloID + ") h cross apply SimulationDB.dbo.MillenniumParticles(63, dbo.Sphere::New(h.x,h.y,h.z," + str(RadiusMax) + ").ToString()) p"
#queryString = "SELECT p.x,p.y,p.z FROM (SELECT x,y,z FROM mpahalotrees..mr WHERE haloId=" + haloID + ") h cross apply SimulationDB.dbo.MillenniumParticles(63, dbo.Sphere::New(h.x,h.y,h.z," + str(RadiusMax) + ").ToString()) p"

context = "SimulationDB"

responseStream = SciServer.CasJobs.executeQuery(queryString, context, token="4abec287b0474397ba2ce69ca78a9efd")

dataFrame = pandas.read_csv(responseStream, index_col=0, dtype={"x": np.float32, "y": np.float32,"z": np.float32})
print(dataFrame)
haloArr = dataFrame.as_matrix(columns=["x","y","z"])
#haloArr = dataFrame.values
#This fixes a lot of problems.
haloArr = np.ascontiguousarray(haloArr, dtype=None)

print(haloArr.shape)
print(haloArr.dtype)
print(haloArr)


mod = SourceModule("""
__global__ void testes(float *dest, float *coords)
{
  const int row =  (blockIdx.x*blockDim.x + threadIdx.x)*3;
  
  const float dist = 0.1;

  if(row < 266144*3){
    
    if(coords[row] > 486.0f){
        dest[row] = 550.0f;
    }else{
        dest[row] = 350.0f;
    }

    dest[row+1] = coords[row+1];
    dest[row+2] = coords[row+2];
      
    

      /*dest[row] = coords[row];
      dest[row+1] = coords[row+1];
      dest[row+2] = coords[row+2];*/

      /*for(int i = 0; i < 1000; i++){
          if(dest[row] < 250.0){
              dest[row] -= dist;
          }else{
              dest[row] += dist;
          }

          if(dest[row+1] < 250.0){
              dest[row+1] += dist;
          }else{
              dest[row+1] -= dist;
          }

          if(dest[row+2] < 250.0){
              dest[row+2] += dist;
          }else{
              dest[row+2] -= dist;
          }

      }*/
  
  
        /*dest[row] = row;
      dest[row+1] = row+1;
      dest[row+2] = row+2;

      dest[row] = coords[row];
      dest[row+1] = coords[row+1];
      dest[row+2] = coords[row+2];*/
  }
}
""")

testes = mod.get_function("testes")

#haloArr = np.ones((266144,3),dtype=np.float32)

dest = np.zeros_like(haloArr)

haloArr_mod = np.empty_like(dest)

dest_gpu = drv.mem_alloc(dest.nbytes)
print(dest.nbytes)
drv.memcpy_htod(dest_gpu, dest)

haloArr_gpu = drv.mem_alloc(haloArr.nbytes)
print(haloArr.nbytes)
print(266144*3*4)
print(266144*3)
drv.memcpy_htod(haloArr_gpu, haloArr)


testes(dest_gpu, haloArr_gpu, block=(1024,1,1), grid=(512,1))

drv.memcpy_dtoh(haloArr_mod, dest_gpu)

print(haloArr_mod)

haloArr_gpu.free()
dest_gpu.free()