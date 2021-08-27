print('\n')
print('Statistic description of train data:')
print(X_train.describe())
print('\n')
print('Statistic description of test data:')
print(X_test.describe())

##Data preprocessing##
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

categorical_columns = ['pclass', 'sex', 'embarked', 'class', 'who', 'deck', 'embark_town', 'alive']
numerical_columns = ['age', 'sibsp', 'parch', 'fare']

categorical_pipe = Pipeline([
    ('imputer', SimpleImputer(strategy='constant', fill_value='most_frequent')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))
])

numerical_pipe = Pipeline([
    ('imputer', SimpleImputer(strategy='mean'))
])

preprocessing = ColumnTransformer(
    [('cat', categorical_pipe, categorical_columns),
     ('num', numerical_pipe, numerical_columns)])

preprocessing.fit(X_train)
X_train_pret = preprocessing.transform(X_train)
X_test_pret = preprocessing.transform(X_test)

print('\n')
print('Dimension of prepared train data :', X_train_pret.shape)
print('Dimension of prepared test data :', X_test_pret.shape)
