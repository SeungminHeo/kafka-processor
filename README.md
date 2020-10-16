# Kafka Processor

카프카로 구현할 기능들의 집합입니다.
---
## Log Simulator
- parameter args 
    - accelerateRate: 시뮬레이터 가속 비율 (ex. 5배)
    - lastDaysFromLast: 9월 21일 부터 몇일 시뮬레이팅 할지 일 수 (ex. 10일)

- 사용법
```
./gradlew runSimulator --args="1000 1"
```
 
- 사용법 with Docker
```

```

## Kafka Streams Processor
- 사용법 
```{java}
// Branch and Preprocessor
./gradlew runStreamProcessor 

// Ranking Processor
TBD

// Matrix Processor
TBD
```
