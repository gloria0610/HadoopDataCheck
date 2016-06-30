package util;

import java.io.File;

public class FileUtil {
	
	public static void delFile(File file){
		if(file.exists()){
			if(file.isFile()){
				file.delete();
			}else{
				File files[]=file.listFiles();
				for(int i=0;i<files.length;i++){
					delFile(files[i]);
				}
			}
			file.delete();
		}
	}

}
