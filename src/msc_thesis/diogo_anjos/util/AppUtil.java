package msc_thesis.diogo_anjos.util;

public class AppUtil {
	  
	private static long bytesToMegabytes(long bytes) {
		  long MEGABYTE = 1024L * 1024L;
		  return bytes / MEGABYTE;
	  }

	public static long getConsumedMemory(){
		Runtime rt = Runtime.getRuntime();
		rt.gc(); //run garbage collector
		return bytesToMegabytes(rt.totalMemory()-rt.freeMemory());
	}
	
	public static long getTotalMemory(){
		Runtime rt = Runtime.getRuntime();
		rt.gc(); //run garbage collector
		return bytesToMegabytes(rt.totalMemory());
	}
	
	public static long getFreeMemory(){
		Runtime rt = Runtime.getRuntime();
		rt.gc(); //run garbage collector
		return bytesToMegabytes(rt.freeMemory());
	}

	public static String getMemoryStatus(){
		return "Memory(MB): Total="+getTotalMemory()+", Used="+getConsumedMemory()+", Free="+getFreeMemory(); 
	}
	
}
