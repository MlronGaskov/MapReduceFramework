# MapReduceFramework

## Оглавление
1. Структура проекта

   - Назначение модуля `mr-api`
   - Назначение модуля `mr-app`
   - Назначение модуля `mr-core`

---
# Структура проекта

Проект состоит из следующих модулей:

# Назначение модуля `mr-api`

Модуль `mr-api` содержит интерфейсы и классы, необходимые для того, чтобы пользователь мог:

- Определить логику этапов **Map** и **Reduce** для своих данных.
- Настроить сериализацию/десериализацию ключей и значений.
- Задать сравнение ключей, хеш-функции и другие параметры, необходимые для корректной работы MapReduce-процесса.

Разработчик, используя `mr-api`, реализует собственные `Mapper` и `Reducer`, а затем, опираясь на предоставленные интерфейсы, создает свой класс, расширяющий `MapReduceJob`. Этот класс станет "описанием задачи" (*Job*), которое затем может быть упаковано в JAR и передано в приложение (использующее ядро фреймворка), чтобы запустить MapReduce-пайплайн.

---

## Интерфейсы и классы `mr-api`

### MapReduceJob
MapReduceJob — это класс, который инкапсулирует все части конкретной MapReduce-задачи: указание конкретных Mapper и Reducer классов, сериализаторов/десериализаторов, компараторов и хеш-функций. Пользователь создает свой класс, например MyJob extends MapReduceJob, указывая нужные параметры. Получившийся "Job" затем компилируется в JAR и передается в приложение, использующее движок фреймворка.
### Mapper
Mapper определяет логику преобразования входных данных (KEY_IN, VALUE_IN) в промежуточные пары (KEY_OUT, VALUE_OUT). На вход map-метод получает итератор входных данных, а результаты записывает в output. Пользователь должен реализовать метод map так, чтобы из исходных данных генерировать пары ключ-значение, которые впоследствии будут сгруппированы и переданы на Reduce-этап.
### Reducer
Reducer обрабатывает все промежуточные значения, сгруппированные по конкретному ключу (KEY_IN), и формирует итоговые результаты (KEY_OUT, VALUE_OUT). Пользователь реализует метод reduce, в котором может агрегировать, подсчитывать статистики или выполнять иные операции над группой значений. Результирующие пары записываются в output.
### (Serializer / Deserializer)
Для корректного обмена данными между этапами Map и Reduce, а также для записи конечного результата, пользователь может задать собственные механизмы сериализации/десериализации для ключей и значений. Это особенно актуально, если используются сложные или нестандартные типы данных.
### KeyHasher и Comparator
KeyHasher определяет функцию хеширования ключей для распределения нагрузки между разными Reducers.
Comparator ключей используется для сортировки промежуточных данных перед фазой Reduce.
### OutputContext
OutputContext используется внутри методов map и reduce для записи результатов. Он предоставляется фреймворком; реализация Mаппера/Редьюсера просто вызывает put, чтобы отправить данные дальше по конвейеру обработки.
### Pair
Простая структура для хранения пар ключ-значение.
### PredefinedFunctions
Набор готовых реализаций сериализаторов, десериализаторов, компараторов и хеш-функций для простых типов данных. Это упрощает старт работы с фреймворком.

## Типичный сценарий использования

1. **Определить Mapper и Reducer**  
   Реализуйте `Mapper` и `Reducer`, указывая типы входных/выходных ключей и значений, а также логику преобразования данных.

2. **Сконфигурировать MapReduceJob**  
   Создайте класс, наследующий `MapReduceJob`, и в его конструкторе укажите:
   - `mapper` и `reducer`
   - `serializer`/`deserializer` для промежуточных и выходных ключей/значений
   - `comparator` для сортировки ключей
   - `hasher` для распределения нагрузок по Reducers

3. **Скомпилировать и упаковать в JAR**  
   Полученный JAR, содержащий ваш кастомный Job, передайте в приложение из модуля mr-app. Приложение при запуске применит заданный Job для обработки входных данных.


# Назначение модуля `mr-app`

Модуль `mr-app` предоставляет точку входа в виде класса `Application`, который читает конфигурацию и дополнительные аргументы командной строки, а затем создаёт и запускает соответствующий компонент — координатор или воркер.

Пример запуска приложения:
```bash
# Запуск в режиме worker с указанием порта
java -jar mr-app-1.0.jar config.yaml worker 8081

# Запуск в режиме coordinator (координатор)
java -jar mr-app-1.0.jar config.yaml coordinator
```

## Пример конфигурационного файла (config.yaml)

Ниже приведён пример структуры конфигурационного файла и описание всех обязательных и опциональных параметров.

```yaml
jarPath: /path/to/custom-job.jar
inputDirectory: /path/to/input/files
mapperOutputDirectory: /path/to/mapper/output
outputDirectory: /path/to/final/output
metricsPort: 8080
mappersCount: 4
reducersCount: 2
logsPath: /path/to/logs
sorterInMemoryRecords: 20000
```
## Параметры конфигурации
- **jarPath** *(строка)*:  
  Путь до пользовательской реализации `MapReduceJob` (JAR-файл, содержащий класс, расширяющий `MapReduceJob`).

- **inputDirectory** *(строка)*:  
  Директория, в которой располагаются исходные входные файлы для маппинга.

- **mapperOutputDirectory** *(строка)*:  
  Директория для хранения промежуточных данных, сгенерированных задачами Map.

- **outputDirectory** *(строка)*:  
  Директория для итоговых выходных файлов после выполнения Reduce-фазы.

- **metricsPort** *(число)*:  
  Порт, по которому координатор откроет HTTP-сервер, по которому будет общаться с воркерами.

- **mappersCount** *(число)*:  
  Количество задач Map, которое следует запустить.

- **reducersCount** *(число)*:  
  Количество задач Reduce.

- **logsPath** *(строка)*:  
  Путь до директории, в которую будут записываться логи работы приложения, координатора и воркеров.

- **sorterInMemoryRecords** *(число, опционально)*:  
  Максимальное число записей, хранимых в памяти при сортировке. Если параметр не указан, по умолчанию используется значение `10000`.

# Назначение модуля `mr-core`

Модуль `mr-core` содержит внутреннюю логику MapReduce-фреймворка.

## Основные классы `mr-core`

Ниже приведено краткое описание некоторых ключевых классов из `mr-core`, которые реализуют внутреннюю логику MapReduce-процесса.

### Coordinator
`Coordinator` отвечает за координацию выполнения MapReduce-задачи. Он принимает регистрацию воркеров и распределяет им задачи.

### Worker
`Worker` реализует логику выполнения отдельных задач Map или Reduce:

- Получает задание от координатора.
- Запускает соответствующие операции с помощью логики, определённой в `MapReduceJob` (Mapper или Reducer).
- Сообщает координатору о завершении задачи и её статусе (успешно или с ошибкой).

### Launcher
`Launcher` — связующий элемент, который по аргументам командной строки определяет роль процесса (координатор или воркер):

- При "coordinator" создаёт экземпляр `Coordinator` и запускает процесс.
- При "worker" создаёт экземпляр `Worker` и ожидает задания от координатора.

### MapReduceTasksRunner
`MapReduceTasksRunner` содержит статические методы для выполнения конкретных этапов задач Map и Reduce. Он:

- Для Map-задачи читает входные данные, применяет `Mapper` и разделяет результаты по заданному числу редьюсеров.
- Для Reduce-задачи объединяет промежуточные результаты, сортирует их и передаёт `Reducer`у для агрегации итоговых данных.

### Вспомогательные классы (sinks, sources, итераторы)
- **sinks** (`FileSink`, `PartitionedFileSink`, `SortedFileSink`): классы, отвечающие за запись результатов (как промежуточных, так и итоговых) в файлы. Они могут производить сортировку и разбиение данных по ключам.
- **sources** (`KeyValueFileIterator`, `GroupedKeyValuesIterator`, `MergedKeyValueIterator`): классы для чтения и группировки ключ-значений из файлов. Они помогают в подготовке данных для Reduce-фазы, обеспечивая объединение входных данных.  

Таким образом, `mr-core` — это "движок" фреймворка, который скрыт от конечного пользователя.