Установка Java 22:

Для Windows:
1. Скачайте установщик:
   - Перейдите на официальный сайт Oracle для загрузки Java Development Kit (JDK).
   - Выберите нужную версию и скачайте установочный файл.

2. Установите JDK:
   - Запустите загруженный установщик и следуйте инструкциям на экране.
   - Выберите путь установки или оставьте по умолчанию.

3. Настройте переменные окружения:
   - Откройте `Панель управления` > `Система` > `Дополнительные параметры системы`.
   - Нажмите `Переменные среды`.
   - В разделе `Системные переменные` найдите переменную `Path` и добавьте путь к папке `bin` установленного JDK.

4. Проверьте установку:
   - Откройте командную строку и введите `java -version`, чтобы убедиться, что новая версия установлена.

Для macOS:
1. Скачайте установщик:
   - Перейдите на официальный сайт Oracle.
   - Скачайте .dmg файл для вашей версии macOS.

2. Установите JDK:
   - Откройте загруженный .dmg файл и перетащите JDK в папку `Программы`.

3. Проверьте установку:
   - Откройте терминал и введите `java -version`.

Для Linux:
1. Скачайте и установите JDK:
   - Для Ubuntu вы можете использовать apt:
     ```bash
     sudo apt update
     sudo apt install openjdk-22-jdk
     ```
   - Для других дистрибутивов используйте соответствующий пакетный менеджер.

2. Проверьте установку:
   - Введите в терминале:
     ```bash
     java -version
     ```

После установки новой версии убедитесь, что ваш проект или приложение настроены на использование новой версии JDK.




Чтобы обновить Java в IntelliJ IDEA, следуйте этим общим шагам.

Проверьте доступные версии JDK:
Перейдите в File > Project Structure (или нажмите Ctrl + Alt + Shift + S).
В разделе Project выберите Project SDK. Если у вас уже установлены другие версии JDK, вы можете увидеть их здесь.

Добавьте новую версию JDK:
Если нужная версия JDK не установлена, нажмите на кнопку New... рядом с Project SDK.
Выберите JDK и укажите путь к новой версии JDK, которую вы установили.

Настройте проект на использование новой версии:
В том же окне Project Structure выберите новый SDK для вашего проекта.
Нажмите OK, чтобы сохранить изменения.

Проверьте настройки компиляции:
Перейдите в File > Settings (или Ctrl + Alt + S).
В разделе Build, Execution, Deployment > Compiler проверьте, что версия компилятора соответствует версии JDK.



Руководство по установке библиотеки MapReduce
1. Открыть проект MapReduceFramework и добавить в MavenLocal. Для этого необходимо открыть gradle на боковой панели справа и нажать publish:
![image](https://github.com/user-attachments/assets/d85a89e8-a7ca-46b3-bec0-6442e008417c)
2. Создаём новый проект, в который будет содержать классы, реализованные пользователем: Mapper, Reducer, MapReduceJob. Изменяем файл build.gradle следующим образом:
```
plugins {
    id 'java'
}

group = 'user'
version = '1.0'

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation 'ru.nsu.mr:mr-core:1.0.0'
    testImplementation platform('org.junit:junit-bom:5.10.0')
    testImplementation 'org.junit.jupiter:junit-jupiter'
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(22)
    }
}

tasks.named('jar') {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from {
        configurations.runtimeClasspath.collect { it.isDirectory() ? it : zipTree(it) }
    }
    manifest {
        attributes 'Main-Class': 'App'
    }
}

test {
    useJUnitPlatform()
}
```

3. Реализовать все необходимые классы(Mapper, Reducer, MapReduceJob), в том числе класс App, он может выглядеть следующим образом:
```
import ru.nsu.mr.*;
import ru.nsu.mr.config.Configuration;

import java.nio.file.Path;
import java.util.List;


public class App {
    public static void main(String[] args) {
        Launcher launcher = new Launcher();
        launcher.launch(new MyMapReduceJob(), new Configuration(),
                Path.of("C:\\Users\\Fedor\\Desktop\\mappers_output"),
                Path.of("C:\\Users\\Fedor\\Desktop\\output"),
                List.of(
                        Path.of("C:\\Users\\Fedor\\Desktop\\input1.txt"),
                        Path.of("C:\\Users\\Fedor\\Desktop\\input2.txt")
                ), args);
    }
}
```

4. Далее необходимо обновить все совместимости(появится чуть левее третьего символа правой боковой панели). Далее открываем боковую панель gradle и нажимаем jar:
![image](https://github.com/user-attachments/assets/77b78405-6ac0-46c9-a7a0-11244828dcdb)

5. Открываем терминал и запускаем worker и coordinator:
![image](https://github.com/user-attachments/assets/fd659b49-394e-44a8-a9fa-73bc84137c3c)
