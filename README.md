## <font color='midblue'>作业6 - 编写MapReduce程序完成文集倒排索引</font>

### 1 作业简述

​        在作业5的数据集基础上完成莎士比亚文集单词的倒排索引，输出按字典序对单词进行排序，单词的索引按照单词在该文档中出现的次数从大到小排序。单词忽略大小写，忽略标点符号（punctuation.txt），忽略停词（stop-word-list.txt），忽略数字，单词长度>=3。

​        输出格式如下：

​        单词1: 文档i#频次a, 文档j#频次b...

​        单词2: 文档m#频次x, 文档n#频次y...

### 2 设计思路

#### 2.1 功能及难点分析

核心功能：

- 对单词进行倒排索引

难点：

- 将单词索引按照单词在该文档中出现的次数从大到小排序


#### 2.2 总体结构

​       Mapper：

```java
public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>
{
    @Override
    public void setup(Context context) throws IOException, InterruptedException{...}
    
    private void parseSkipFile(String fileName){...}
    
    @Override
    public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException{...}    
}
```

​       Reducer：

```java
public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>
{
    private class ValueComparator implements Comparator<Map.Entry<String, Integer>>{...}
    
    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException{...}
}
```

​        Combiner：

```java
public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException{...}
}
```

​        main 函数：

```java
public static void main(String[] args){...}
```

#### 2.3 具体实现细节

​       忽略大小写（map函数）：将读入的字符串使用 toLowerCase() 全部转换为小写

```Java
  String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
```

​       过滤标点符号（map函数）：在 parseSkipFile() 函数中将 punctuation.txt 中的标点符号读入 HashSet 中，在读文本的过程中将其替换为空格。

```Java
  for (String pattern : patternsToSkip)
  {
      line = line.replaceAll(pattern, " ");
  }
```

​       忽略数字（map函数）：使用正则表达式过滤数字

```Java
  Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
```

​       忽略停用词（map函数）：

```Java
  /*读取停用词:*/
  File fileStopWord = new File("stop-word-list.txt");
  try
  {
      scanner = new Scanner(fileStopWord);
      String stringStopWord = scanner.nextLine();
      for (; scanner.hasNextLine(); stringStopWord = scanner.nextLine())
                    stopWordsSet.add(stringStopWord);
  }
  catch (FileNotFoundException e)
  {
      e.printStackTrace();
  }
  ...
  while (itr.hasMoreTokens())
  {
      String hhh=itr.nextToken();
      keyInfo.set(hhh);
      //如果是数字则不保存
      if (pattern.matcher(hhh).matches())
      {
          continue;
      }
      if(!stopWordsSet.contains(hhh) && keyInfo.getLength()>=3)
      {
          // key值由单词和所处文件名组成,即word:position
          keyInfo.set(hhh + ":"+fileName);
          //词频初始为1
          valueInfo.set("1");
          context.write(keyInfo, valueInfo);
      }
  }
```

​       倒排索引的实现：

- 在 Mapper 中重新定义 key：word -> word：filename；
- 在 Combiner 中统计新 key （即 word: filename）的词频，并重新设置 value 为 filename#sum，重新设置 key 为 word；
- 在 Reducer 中对相同 key 的 value 进行字符串拼接。

```java
public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>
{
     private Text info = new Text();
     @Override
     protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException
     {
          //统计词频,key相同,对值求和,即word:position的词频
          int sum = 0;
          for (Text value : values)
          {
              sum += Integer.parseInt(value.toString());
          }
          int splitIndex = key.toString().indexOf(":");
          //重新设置value值由URI和词频组成 position:sum
          info.set(key.toString().substring(splitIndex + 1) +"#"+sum);
          //重新设置key值为单词
          key.set(key.toString().substring(0,splitIndex));
          //key.set(key.toString().substring(0,splitIndex) + " " + sum);
          context.write(key,info);
        }
    }
```

​        将单词索引按照单词在该文档中出现的次数从大到小排序：在 Reducer 中将相同 key 的不同 value（filename#sum）分割为键值对<filename,sum>放入 HashMap 中，将 map 的 entryset 放入 list 集合，定义 ValueComparator 类实现 list 集合中键值对的排序，用迭代器对list中的键值对元素进行遍历并组合为字符串写入输出文件中。

```Java
public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
{
    private Text result = new Text();

    private class ValueComparator implements Comparator<Map.Entry<String, Integer>>
    {
        public int compare(Map.Entry<String, Integer> mp1, Map.Entry<String, Integer> mp2)
        {
            return mp2.getValue() - mp1.getValue();
        }
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException
    {
        //产生一个map并添加一些参数
        Map<String,Integer> map = new HashMap<>();
        for (Text value : values)
        {
            int splitIndex = value.toString().indexOf("#");
            String key1 = value.toString().substring(0,splitIndex);
            Integer value1 = Integer.parseInt(value.toString().substring(splitIndex+1));
            map.put(key1, value1);
        }
        //将map的entryset放入list集合
        List<Map.Entry<String,Integer>> list = new ArrayList<>(map.entrySet());  
        //对list进行排序,并通过Comparator传入自定义的排序规则
        ValueComparator vc=new ValueComparator();
        Collections.sort(list,vc);
        //用迭代器对list中的键值对元素进行遍历
        Iterator<Map.Entry<String, Integer>> iter = list.iterator();
        //生成文档列表
        String fileList = "";
        while(iter.hasNext())
        {
             Map.Entry<String, Integer> item = iter.next();
             String key2 = item.getKey();
             int value2 = item.getValue();
             String value3 = value2 + "";
             fileList += key2+"#"+value3+",";
         }
         result.set(fileList);
         context.write(key, result);
    }
}
```

​       自定义<key,value>的输出分隔符为":"：

```Java
 conf.set("mapred.textoutputformat.ignoreseparator", "true");
 conf.set("mapred.textoutputformat.separator", ":");
```

​    

### 3 实验结果

- **InvertedIndex/output/part-r-00000.txt（部分）**

```
aaron:shakespeare-titus-50.txt#98,
abaissiez:shakespeare-life-54.txt#1,
abandon:shakespeare-as-12.txt#4,shakespeare-twelfth-20.txt#1,shakespeare-timon-49.txt#1,shakespeare-taming-2.txt#1,shakespeare-third-53.txt#1,shakespeare-othello-47.txt#1,shakespeare-troilus-22.txt#1,
abandoned:shakespeare-alls-11.txt#1,shakespeare-titus-50.txt#1,
abase:shakespeare-tragedy-58.txt#1,
abash:shakespeare-troilus-22.txt#1,
abate:shakespeare-life-54.txt#5,shakespeare-venus-60.txt#1,shakespeare-cymbeline-17.txt#1,shakespeare-hamlet-25.txt#1,shakespeare-merchant-5.txt#1,shakespeare-taming-2.txt#1,shakespeare-tragedy-58.txt#1,shakespeare-romeo-48.txt#1,shakespeare-midsummer-16.txt#1,shakespeare-loves-8.txt#1,shakespeare-titus-50.txt#1,
abated:shakespeare-coriolanus-24.txt#1,shakespeare-king-45.txt#1,shakespeare-second-52.txt#1,
abatement:shakespeare-twelfth-20.txt#1,shakespeare-cymbeline-17.txt#1,shakespeare-king-45.txt#1,
abatements:shakespeare-hamlet-25.txt#1,
abates:shakespeare-tempest-4.txt#1,
abbess:shakespeare-comedy-7.txt#8,
abbey:shakespeare-comedy-7.txt#9,shakespeare-life-56.txt#3,shakespeare-two-18.txt#2,shakespeare-life-55.txt#2,shakespeare-romeo-48.txt#1,shakespeare-second-52.txt#1,
abbeys:shakespeare-life-56.txt#1,
abbominable:shakespeare-loves-8.txt#1,
abbot:shakespeare-tragedy-57.txt#8,shakespeare-life-55.txt#2,
abbots:shakespeare-life-56.txt#1,
abbreviated:shakespeare-loves-8.txt#1,
abed:shakespeare-twelfth-20.txt#1,shakespeare-coriolanus-24.txt#1,shakespeare-as-12.txt#1,shakespeare-alls-11.txt#1,
abel:shakespeare-tragedy-57.txt#1,
abergavenny:shakespeare-life-55.txt#11,
abet:shakespeare-tragedy-57.txt#1,
abetting:shakespeare-comedy-7.txt#1,
abettor:shakespeare-rape-61.txt#1,
abhominable:shakespeare-loves-8.txt#1,
```



- **提交作业运行成功的WEB页面截图（使用BDKIT）**

![](C:\Users\THINK\Desktop\屏幕截图 2021-10-28 183309.jpg)

![](C:\Users\THINK\Desktop\屏幕截图 2021-10-28 140501.jpg)



### 4 收获

​        通过这次作业，我掌握了用MapReduce实现倒排索引的基本方法，对于 java 各种数据类型之间的转换越来越擅长，对于 Mapper， Reducer，Combiner 各自的功能也有了更深刻的认识，相信今后我对 MapReduce 以及 Hadoop 相关知识的掌握会越来越好！
