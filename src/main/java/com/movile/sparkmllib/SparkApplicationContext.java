package com.movile.sparkmllib;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * J.P. Eiti Kimura (eiti.kimura@movile.com)
 * 02/03/16.
 */
public final class SparkApplicationContext implements Closeable {

    private JavaSparkContext sc;

    public SparkApplicationContext(String appName) {
        this.sc = new JavaSparkContext("local[*]", appName, "", "");
    }

    public JavaSparkContext getContext() {
        return this.sc;
    }

    public SparkContext sc() {
        return this.sc.sc();
    }

    public DecisionTreeModel loadModel(Path path) {
        return DecisionTreeModel.load(this.sc(), path.toString());
    }

    @Override
    public void close() throws IOException {
        sc.close();
        sc.stop();
    }
}
