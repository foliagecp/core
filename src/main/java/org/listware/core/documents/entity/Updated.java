/* Copyright 2022 Listware */

package org.listware.core.documents.entity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.fasterxml.jackson.annotation.JacksonAnnotationsInside;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

//TODO: in v7 add targets ElementType.METHOD and ElementType.PARAMETER
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@JacksonAnnotationsInside
@JsonProperty(DocumentFields.UPDATED)
@JsonInclude(JsonInclude.Include.NON_NULL)
public @interface Updated {
}