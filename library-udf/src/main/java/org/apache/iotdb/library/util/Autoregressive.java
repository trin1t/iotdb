package org.apache.iotdb.library.util;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;

import java.util.ArrayList;

public class Autoregressive {
    private double[] originalValue = {};
    private int p;
    private double[] coefficient = {};
    private double[] fitSeq = {};
    private double[] residual = {};

    public Autoregressive(double[] value, int p) {
        this.originalValue = value;
        this.p = p;
        this.calculateCoefficients();
        this.calculateFitSeq();
        this.calculateResidual();
    }

    public double get_sum(double[] dataArray)
    {
        double sumData = 0;
        for (double v : dataArray) {
            sumData += v;
        }
        return sumData;
    }

    public double get_avg(double[] dataArray)
    {
        return get_sum(dataArray) / dataArray.length;
    }


    public double get_variance(double[] dataArray)
    {
        double variance = 0;
        double avg = get_avg(dataArray);

        for(int i = 0; i < dataArray.length; i++)
        {
            double temp = dataArray[i] - avg;
            variance += temp * temp;
        }
        return variance / dataArray.length;
    }

    public double[] get_autocorr(double[] dataArray, int order) {
        double[] autoCor = new double[order + 1];
        double varData = get_variance(dataArray);

        for (int i = 0; i <= order; i++) {
            autoCor[i] = 0;
            for (int j = 0; j < dataArray.length - i; j++) {
                autoCor[i] += dataArray[j + i] * dataArray[j];
            }
            autoCor[i] = autoCor[i] / (dataArray.length - i);
            autoCor[i] = autoCor[i] / varData;
        }
        return autoCor;
    }

    public double[] get_autocorr_grma(double[] dataArray, int order) {
        double[] autoCor = new double[order + 1];
        for (int i = 0; i <= order; i++) {
            autoCor[i] = 0;
            for (int j = 0; j < dataArray.length - i; j++) {
                autoCor[i] += dataArray[j + i] * dataArray[j];
            }
            autoCor[i] /= (dataArray.length - i);
        }
        return autoCor;
    }

    private void calculateCoefficients() {
        double[][] toeplitz = new double[p][p];
        double[] atuocorr = get_autocorr(originalValue,p);
        double[] autocorrF = get_autocorr_grma(originalValue, p);
        for(int i = 1; i <= p; i++)
        {
            int k = 1;
            for(int j = i - 1; j > 0; j--)
            {
                toeplitz[i-1][j-1] = atuocorr[k++];
            }
            toeplitz[i-1][i-1] = atuocorr[0];
            int kk=1;
            for(int j=i;j<p;j++)
            {
                toeplitz[i-1][j]=atuocorr[kk++];
            }
        }

        RealMatrix toplizeMatrix = new Array2DRowRealMatrix(toeplitz);
        RealMatrix toplizeMatrixinverse = new LUDecomposition(toplizeMatrix).getSolver().getInverse();

        double[] temp=new double[p];
        if (p >= 0) System.arraycopy(atuocorr, 1, temp, 0, p);

        RealMatrix autocorrMatrix = new Array2DRowRealMatrix(temp);
        RealMatrix parautocorDataMatrix = toplizeMatrixinverse.multiply(autocorrMatrix);

        this.coefficient = new double[parautocorDataMatrix.getRowDimension()];
        for(int i = 0; i < parautocorDataMatrix.getRowDimension(); i++)
        {
            this.coefficient[i]=parautocorDataMatrix.getEntry(i,0);
        }
    }

    private void calculateFitSeq() {
        this.fitSeq = new double[originalValue.length];
        for (int i = 0; i < originalValue.length; i++) {
            if (i >= p) {
                double predict = 0.0;
                for (int j = i - 1; j >= i - p; j --) {
                    predict += originalValue[j] * this.coefficient[i - j - 1];
                }
                this.fitSeq[i] = predict;
            }
            else {
                this.fitSeq[i] = originalValue[i];
            }
        }
    }

    private void calculateResidual() {
        this.residual = new double[originalValue.length];
        for (int i = 0; i < originalValue.length; i++) {
            residual[i] = this.originalValue[i] - this.fitSeq[i];
        }
    }

    public double[] forecast(int steps) {
        double [] forecastSeq = new double[steps];
        int originalLen = originalValue.length;
        for (int i = 0; i < steps; i ++) {
            int current_pos = originalLen + i;
            if (current_pos >= p) {
                double predict = 0.0;
                for (int j = current_pos - 1; j >= current_pos - p; j --) {
                    if (j >= originalLen) {
                        predict += forecastSeq[j - originalLen] * this.coefficient[current_pos - j - 1];
                    }
                    else{
                        predict += originalValue[j] * this.coefficient[current_pos - j - 1];
                    }
                }
                forecastSeq[i] = predict;
            }
            else {
                forecastSeq[i] = 0;
            }
        }
        return forecastSeq;
    }

    public double[] getResidual() {
        return this.residual;
    }

    public double[] getCoefficient() {
        return this.coefficient;
    }

    public double[] getFitSeq() {
        return this.fitSeq;
    }
}
