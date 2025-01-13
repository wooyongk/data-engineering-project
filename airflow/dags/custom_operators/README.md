## SQLColumnValueExistenceCheckOperator

`SQLColumnValueExistenceCheckOperator` 는 Airflow의 `BaseSQLOperator`를 확장한 사용자 정의 연산자로,
특정 SQL 테이블의 지정된 열에 특정 값들이 존재하는지 확인하는 기능을 제공합니다.
연산자는 주어진 테이블과 열 이름, 그리고 확인할 값들을 매개변수로 받아, 각각의 값이 해당 열에 존재하는지 여부를 검사합니다.

## HolidayOperator

`HolidayOperator`는 Airflow에서 데이터 데이터베이스에 저장된 휴일 정보를 기반으로 DAG의 실행 흐름을 동적으로 제어하는 Custom Operator입니다.
휴일 정보에 따라 특정 태스크를 실행하거나 건너뛸 수 있어 유연한 스케줄 관리를 지원합니다.