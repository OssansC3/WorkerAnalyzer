package workerAnalyzer;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WorkerAnalyzer1 {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(WorkerAnalyzer1.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2014004");                   // ★自分の学籍番号

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 入出力ファイルを指定
		String inputpath = "posdata";
		String outputpath = "out/worker1";     // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(8);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}



	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			//明らかなノイズ日は除く
			if(isNoisyDay(csv[PosUtils.MONTH],csv[PosUtils.DATE]))return;

			//サラリーマン対象
			if(!isEqual(csv[PosUtils.BUYER_AGE],3,4)) return;

			//時間は朝か夜
			String hour = csv[PosUtils.HOUR];
			if(!isRange(hour,6,10)&& //朝
					!isRange(hour,19,23)) return; //夜

			String week = csv[PosUtils.WEEK];

			//期間はいつでもおｋ
			//if(isEqual(csv[PosUtils.MONTH],8)) vacation = true;

			// valueとなる売り上げ
			String count = csv[PosUtils.ITEM_TOTAL_PRICE];

			// keyとなる分類名
			String name = csv[PosUtils.ITEM_CATEGORY_NAME];

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new Text(name), new Text(format(count,week,hour)));
		}

		private static String format(String count,String week,String hour){
			if(isRange(hour,6,10)) return week+",0,"+count;
			else return week+",1,"+count;
		}

		private static boolean isEqual(String str,int num){
			return Integer.valueOf(str)==num;
		}
		private static boolean isEqual(String str,int num1,int num2){
			return (Integer.valueOf(str)==num1)||(Integer.valueOf(str)==num2);
		}

		private static boolean isRange(String str,int num1,int num2){
			return (Integer.valueOf(str)>=num1)&&(Integer.valueOf(str)<=num2);
		}

		private static boolean isNoisyDay(String month,String date){
			if((isEqual(month,1)&&isEqual(date,1))||
					(isEqual(month,12)&&isEqual(date,24,25))||
					(isEqual(month,2)&&isEqual(date,14))||
					(isEqual(month,3)&&isEqual(date,14))) return true;
			else return false;
		}

	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private static final int WEEK = 0;
		private static final int TIME = 1;
		private static final int COUNT = 2;
		private static final int LENGTH= 7;

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// 売り上げを合計
			long countMorning = 0,countNight = 0,
					countWeekStart = 0, countWeekEnd = 0,
					countSutStart = 0, countSutEnd = 0,
					countSunStart = 0, countSunEnd = 0;
			for (Text value : values) {
				String list[] = value.toString().split(",");
				long count = Long.valueOf(list[COUNT]);
				if(list[TIME].equals("0")){
					countMorning += count;
					if(list[WEEK].equals("1")) countWeekStart += count;
					if(list[WEEK].equals("6")) countSutStart += count;
					if(list[WEEK].equals("7")) countSunStart += count;
				} else {
					countNight += count;
					if(list[WEEK].equals("5")) countWeekEnd += count;
					if(list[WEEK].equals("6")) countSutEnd += count;
					if(list[WEEK].equals("7")) countSunEnd += count;
				}
			}

			if((countMorning<10000)&&(countNight<10000)) return;

			// emit
			context.write(new Text(key),new Text(format(countMorning,countNight,countWeekStart,countWeekEnd,
					countSutStart,countSutEnd,countSunStart,countSunEnd)));
		}

		private static String format(long m,long n,long s,long e,long s6,long e6,long s7,long e7){
			long mAvg = m/LENGTH, nAvg = n/LENGTH;
			String ss = PorM("週初",s,mAvg);
			String es = PorM("週末",e,nAvg);
			String s6s = PorM("土初",s6,mAvg);
			String e6s = PorM("土終",e6,nAvg);
			String s7s = PorM("日初",s7,mAvg);
			String e7s = PorM("日終",e7,nAvg);

			return ss+"\t"+es+"\t"+s6s+"\t"+e6s+"\t"+s7s+"\t"+e7s+"\n-----\t"
				+s+"\t"+s6+"\t"+s7+"\t"+mAvg+"\t"+m+"\t"
				+e+"\t"+e6+"\t"+e7+"\t"+nAvg+"\t"+n;
		}

		private static String PorM(String str,long n,long avg){
			if(n >= avg*2) return str+"+++";
			else if(n >= avg*1.6) return str+"++ ";
			else if(n >= avg*1.3) return str+"+  ";
			else if(n <= avg/2) return str+"---";
			else if(n <= avg/1.6) return str+"-- ";
			else if(n <= avg/1.3) return str+"-  ";
			else return str+"   ";
		}
	}

}

