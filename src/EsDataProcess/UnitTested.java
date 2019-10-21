package EsDataProcess;
import java.lang.annotation.*;

/**
 * Stable APIs that retain source and binary compatibility within a major release.
 * These interfaces can change from one major release to another major release
 * (e.g. from 1.0 to 2.0).
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER,
  ElementType.CONSTRUCTOR, ElementType.LOCAL_VARIABLE, ElementType.PACKAGE})
public @interface UnitTested {}
