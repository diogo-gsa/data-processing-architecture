package msc_thesis.diogo_anjos.util;

public class AppUtil {
	  
	private static long bytesToMegabytes(long bytes) {
		  long MEGABYTE = 1024L * 1024L;
		  return bytes / MEGABYTE;
	  }

	public static long getConsumedMemory(boolean preRunGarbageCollector){
		Runtime rt = Runtime.getRuntime();
		if(preRunGarbageCollector){
			rt.gc(); //run garbage collector
		}
		return bytesToMegabytes(rt.totalMemory()-rt.freeMemory());
	}
	
	public static long getTotalMemory(boolean preRunGarbageCollector){
		Runtime rt = Runtime.getRuntime();
		if(preRunGarbageCollector){
			rt.gc(); //run garbage collector
		}
		return bytesToMegabytes(rt.totalMemory());
	}
	
	public static long getFreeMemory(boolean preRunGarbageCollector){
		Runtime rt = Runtime.getRuntime();
		if(preRunGarbageCollector){
			rt.gc(); //run garbage collector
		}
		return bytesToMegabytes(rt.freeMemory());
	}

	public static String getMemoryStatus(boolean preRunGarbageCollector){
		if(preRunGarbageCollector){
			Runtime rt = Runtime.getRuntime();
			rt.gc();
		}
		return "Memory(MB): Total="+getTotalMemory(false)+", Used="+getConsumedMemory(false)+", Free="+getFreeMemory(false); 
	}	
}
