package org.hy.hadoop.countexcel;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

/**
 * 解析Excel导入数据 需要导入poi-3.9.jar, poi-excelant-3.9.jar
 * 
 * @author andy
 * @version 1.0
 * 
 */
public class ExcelParser {

	private static final Log LOG = LogFactory.getLog(ExcelParser.class);
	private StringBuilder currentString = null;
	private long bytesRead = 0;

	public String parseExcelData(InputStream is) {
		try {
			HSSFWorkbook workbook = new HSSFWorkbook(is);

			// taking the first sheet from the workbook
			HSSFSheet sheet = workbook.getSheetAt(0);

			// iterate through each rows from first sheet
			Iterator<Row> rowIterator = sheet.iterator();
			currentString = new StringBuilder();
			while (rowIterator.hasNext()) {
				Row row = (Row) rowIterator.next();

				// for each row, iterator through each columns
				Iterator<Cell> cellIterator = row.cellIterator();
				while (cellIterator.hasNext()) {
					Cell cell = (Cell) cellIterator.next();

					switch (cell.getCellType()) {
					case Cell.CELL_TYPE_BOOLEAN:
						bytesRead++;
						currentString.append(cell.getBooleanCellValue() + "\t");
						break;
					case Cell.CELL_TYPE_NUMERIC:
						bytesRead++;
						currentString.append(cell.getNumericCellValue() + "\t");
						break;
					case Cell.CELL_TYPE_STRING:
						bytesRead++;
						currentString.append(cell.getStringCellValue() + "\t");
						break;

					default:
						break;
					}
				}
				
				currentString.append("\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("IO Exception : File not found " + e);
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return currentString.toString();

	}
	
	public long getBytesRead(){
		return bytesRead;
	}

}
