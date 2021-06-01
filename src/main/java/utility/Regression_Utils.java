package utility;

import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.*;
import scala.Tuple2;

import org.apache.spark.ml.feature.VectorAssembler;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Regression_Utils {
    public static Tuple2<String, Integer> linearRegression(Dataset<Row> train, String month) {

        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

        VectorAssembler va = new VectorAssembler()
                .setInputCols(new String[] {"data"})
                .setOutputCol("features");

        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .setFeaturesCol("features")
                .setLabelCol("total");

        LinearRegressionModel lrModel = lr.fit(va.transform(train));
        Date monthConv = null;
        try {
            monthConv = sdf1.parse(month);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar c = new GregorianCalendar(Locale.ITALIAN);
        c.setTime(monthConv);
        c.add(Calendar.MONTH, 1);
        Date newMonth = c.getTime();
        String newStr = sdf2.format(newMonth);

        double predict = lrModel.predict(Vectors.dense(newMonth.getTime()));
        // 1 MM, num
        return new Tuple2<>(newStr, (int) predict);


       /* VectorAssembler vaTesting = new VectorAssembler()
                .setInputCols(new String[] {"age", "area"})
                .setOutputCol("features");*/

     /*   Dataset<Row> adjTraining = vaTraining.transform(dsTraining);
        Dataset<Row> labTraining = adjTraining.select("features", "total");
        Dataset<Row> trainingData = labTraining.withColumnRenamed("total", "label");

        Dataset<Row> adjTesting = vaTesting.transform(dsTesting);
        Dataset<Row> testingData = adjTesting.select("data", "features");
       //


       // LinearRegressionModel lrModel = lr.fit(vaTraining.transform(dsTraining));
      /*  // Fit the model
        LinearRegressionModel lrModel = lr.fit(trainingData);
        Dataset<Row> predictions = lrModel.transform(testingData);
        predictions.show();
*/


    }

}