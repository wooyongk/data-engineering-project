## SQLColumnValueExistenceCheckOperator

---

`SQLColumnValueExistenceCheckOperator` 는 Airflow의 `BaseSQLOperator`를 확장한 사용자 정의 연산자로,
특정 SQL 테이블의 지정된 열에 특정 값들이 존재하는지 확인하는 기능을 제공합니다.
연산자는 주어진 테이블과 열 이름, 그리고 확인할 값들을 매개변수로 받아, 각각의 값이 해당 열에 존재하는지 여부를 검사합니다.