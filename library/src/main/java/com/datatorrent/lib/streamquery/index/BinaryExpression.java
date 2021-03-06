/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.streamquery.index;

import javax.validation.constraints.NotNull;


/**
 * Abstract class to filter row by binary expression index. 
 * <p>
 * Sub class will implement filter/getExpressionName functions.
 * @displayName Binary Expression
 * @category Streamquery/Index
 * @tags alias
 * @since 0.3.4
 */
abstract public class BinaryExpression  implements Index
{
  /**
   * Left column name argument for expression.
   */
  @NotNull
  protected String left;
  
  /**
   * Right column name argument for expression.
   */
  @NotNull
  protected String right;
  
  /**
   *  Alias name for output field.
   */
  protected String alias;

  /**
   * @param Left column name argument for expression.
   * @param Right column name argument for expression.
   * @param Alias name for output field.
   */
  public BinaryExpression(@NotNull String left, @NotNull String right, String alias) 
  {
    this.left = left;
    this.right = right;
  }

  public String getAlias()
  {
    return alias;
  }

  public void setAlias(String alias)
  {
    this.alias = alias;
  }
}
