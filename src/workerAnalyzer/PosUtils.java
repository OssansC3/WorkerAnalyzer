package workerAnalyzer;


import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class PosUtils {

	/**
	 * POSファイルの列の総数
	 */
	public static final int N_ROWS = 20;

	/**
	 * POSファイル中の「店舗立地」のCSV列番号
	 */
	public static final int LOCATION = 0;
	/**
	 * POSファイル中の「レシート番号」のCSV列番号
	 */
	public static final int RECEIPT_ID = 1;
	/**
	 * POSファイル中の「YYYY」のCSV列番号
	 */
	public static final int YEAR = 2;
	/**
	 * POSファイル中の「MM」のCSV列番号
	 */
	public static final int MONTH = 3;
	/**
	 * POSファイル中の「DD」のCSV列番号
	 */
	public static final int DATE = 4;
	/**
	 * POSファイル中の「曜日フラグ」のCSV列番号
	 */
	public static final int WEEK = 5;
	/**
	 * POSファイル中の「休日フラグ」のCSV列番号
	 */
	public static final int IS_HOLIDAY = 6;
	/**
	 * POSファイル中の「hh」のCSV列番号
	 */
	public static final int HOUR = 7;
	/**
	 * POSファイル中の「mm」のCSV列番号
	 */
	public static final int MINUTE = 8;
	/**
	 * POSファイル中の「ss」のCSV列番号
	 */
	public static final int SECOND = 9;
	/**
	 * POSファイル中の「購入者性別フラグ」のCSV列番号
	 */
	public static final int BUYER_SEX = 10;
	/**
	 * POSファイル中の「購入者年齢フラグ」のCSV列番号
	 */
	public static final int BUYER_AGE = 11;
	/**
	 * POSファイル中の「購入商品名」のCSV列番号
	 */
	public static final int ITEM_NAME = 12;
	/**
	 * POSファイル中の「JANコード」のCSV列番号
	 */
	public static final int ITEM_JANCODE = 13;
	/**
	 * POSファイル中の「単価」のCSV列番号
	 */
	public static final int ITEM_PRICE = 14;
	/**
	 * POSファイル中の「個数」のCSV列番号
	 */
	public static final int ITEM_COUNT = 15;
	/**
	 * POSファイル中の「値段」のCSV列番号
	 */
	public static final int ITEM_TOTAL_PRICE = 16;
	/**
	 * POSファイル中の「メーカー名」のCSV列番号
	 */
	public static final int ITEM_MADEBY = 17;
	/**
	 * POSファイル中の「分類コード」のCSV列番号
	 */
	public static final int ITEM_CATEGORY_CODE = 18;
	/**
	 * POSファイル中の「分類名」のCSV列番号
	 */
	public static final int ITEM_CATEGORY_NAME = 19;


	/**
	 * MRの出力フォルダを削除する．
	 * （出力フォルダの上書きエラーを避けるため）
	 *
	 * ローカルHadoop環境とリモートHadoop環境のどちらでも対応可能
	 *
	 * @param outputDir 出力フォルダへのパス
	 */
	public static void deleteOutputDir(String outputDir) {
		try {
			// ローカルHDP環境の出力フォルダを削除
			FileUtils.forceDelete(new File(outputDir));
		} catch (IOException e) {}
		try {
			// サーバHDP環境の出力フォルダを削除
			FileSystem fs = FileSystem.get(new Configuration());
			fs.delete(new Path(outputDir), true);
		} catch (IOException e) {}
	}

	public static Text convertToText(String from) {
		return new Text(from);
	}

	public static Text convertToText(int from) {
		return new Text(String.valueOf(from));
	}

	public static Text convertToText(double from) {
		return new Text(String.valueOf(from));
	}
}
