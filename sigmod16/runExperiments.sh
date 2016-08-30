## Merge Tree Index Performance
cd performance-evaluation/merge-tree-index/
python run.py
python merge-tree-index-performance.py

## Feature Indexing and Identification, and Query Performance
cd ../nyc-open/
./run-varying > run-varying.out
python running-time-preprocessing.py metadata/ run-varying.out False nyc-open-metadata
python running-time-relationship.py metadata/ run-varying.out False nyc-open-metadata
cd ../nyc-urban/
./run-varying > run-varying.out
python running-time-preprocessing.py metadata/ run-varying.out True
python running-time-relationship.py metadata/ run-varying.out True

## Relationship Pruning
./download-relationships
cd ../nyc-open/
./download-relationships
cd ../nyc-urban/pruning/
./get-pruning-data
python pruning.py results events restricted week city True
cd ../../nyc-open/pruning/
./get-pruning-data
python pruning.py results events restricted week city False

## Correctness
cd ../../../
./correctness

## Robustness
cd robustness/
./robustness
python robustness.py noise-exp-taxi-city.out 1D False

## Standard Techniques
cd ../
./standard-techniques