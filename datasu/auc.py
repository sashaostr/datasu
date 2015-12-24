__author__ = 'sasha'

import matplotlib.pyplot as plt
import sklearn.cross_validation as cv
from sklearn.metrics import roc_curve, auc
from sklearn.cross_validation import StratifiedKFold
from scipy import interp
import numpy as np

def plot_auc(y_score, y_true):
    # Compute ROC curve and area the curve
    fpr, tpr, thresholds = roc_curve(y_score, y_true)

    roc_auc = auc(fpr, tpr)
    plt.plot(fpr, tpr, lw=1, label='ROC (area = %0.2f)' % (roc_auc))

    plt.plot([0, 1], [0, 1], '--', color=(0.6, 0.6, 0.6), label='Luck')

    plt.xlim([-0.05, 1.05])
    plt.ylim([-0.05, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver operating characteristic')
    plt.legend(loc="lower right")
    plt.show()




def run_crossval(X,y, fitter):

    # Classification and ROC analysis

    # Run classifier with cross-validation and plot ROC curves
    skf_cv = cv.StratifiedKFold(y, n_folds=5) #n_folds=6
#     classifier = svm.Lo(kernel='linear', probability=True,random_state=random_state)

    mean_tpr = 0.0
    mean_fpr = np.linspace(0, 1, 100)
    all_tpr = []

    for i, (train, test) in enumerate(skf_cv):
        model = fitter.fit(X.iloc[train], y.iloc[train])
#         probas_ = model.predict_proba(X.iloc[test])
#         fpr, tpr, thresholds = roc_curve(y.iloc[test], probas_[:, 1])
        probas_ = model.decision_function(X.iloc[test])
        fpr, tpr, thresholds = roc_curve(y.iloc[test], probas_)
        mean_tpr += interp(mean_fpr, fpr, tpr)
        mean_tpr[0] = 0.0
        roc_auc = auc(fpr, tpr)
        plt.plot(fpr, tpr, lw=1, label='ROC fold %d (area = %0.2f)' % (i, roc_auc))

    plt.plot([0, 1], [0, 1], '--', color=(0.6, 0.6, 0.6), label='Luck')

    mean_tpr /= len(skf_cv)
    mean_tpr[-1] = 1.0
    mean_auc = auc(mean_fpr, mean_tpr)
    plt.plot(mean_fpr, mean_tpr, 'k--',
             label='Mean ROC (area = %0.2f)' % mean_auc, lw=2)

    plt.xlim([-0.05, 1.05])
    plt.ylim([-0.05, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver operating characteristic')
    plt.legend(loc="lower right")
    plt.show()
    return mean_auc