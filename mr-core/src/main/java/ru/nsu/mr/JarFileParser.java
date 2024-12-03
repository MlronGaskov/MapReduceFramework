package ru.nsu.mr;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class JarFileParser {
    private final URLClassLoader classLoader;
    private final Path jarFilePath; // Сохранение пути файла JAR

    public JarFileParser(Path jarFilePath) throws IOException {
        this.jarFilePath = jarFilePath; // Установка пути JAR файла
        File jarFile = jarFilePath.toFile();
        URL jarUrl = jarFile.toURI().toURL();
        classLoader = new URLClassLoader(new URL[]{jarUrl}, getClass().getClassLoader());
    }

    public <T> T loadUsersSubClass(Class<T> superClass) throws Exception {
        List<Class<? extends T>> subClasses = findSubClasses(superClass);

        if (subClasses.isEmpty()) {
            throw new ClassNotFoundException("No subclass of " + superClass.getSimpleName() + " found in JAR.");
        } else if (subClasses.size() > 1) {
            throw new IllegalStateException("More than one subclass of " + superClass.getSimpleName() + " found in JAR: " + subClasses);
        }

        Class<? extends T> subClass = subClasses.get(0);
        return subClass.getDeclaredConstructor().newInstance(); // Создаем экземпляр класса
    }

    private <T> List<Class<? extends T>> findSubClasses(Class<T> superclass) throws IOException {
        List<Class<? extends T>> subClasses = new ArrayList<>();

        // Открываем JAR файл и перебираем его содержимое
        try (JarFile jarFile = new JarFile(new File(jarFilePath.toUri()))) {
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (entry.getName().endsWith(".class")) {
                    // Получаем имя класса без .class
                    String className = entry.getName().replace("/", ".").replace(".class", "");

                    try {
                        // Загружаем класс
                        Class<?> loadedClass = classLoader.loadClass(className);
                        // Проверяем, является ли класс подклассом указанного класса
                        if (superclass.isAssignableFrom(loadedClass) && !loadedClass.equals(superclass)) {
                            subClasses.add((Class<? extends T>) loadedClass);
                        }
                    } catch (ClassNotFoundException | NoClassDefFoundError e) {
                        // Игнорируем ошибки при загрузке
                    }
                }
            }
        }

        return subClasses; // Возвращаем найденные подклассы
    }

    public void close() throws IOException {
        classLoader.close();
    }
}
