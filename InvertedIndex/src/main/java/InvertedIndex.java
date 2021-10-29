import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class InvertedIndex
{
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text keyInfo = new Text();     // 存储单词和URI的组合
        private Text valueInfo = new Text();   //存储词频
        private FileSplit split;               // 存储split对象
        private Configuration conf;
        private boolean caseSensitive;
        private BufferedReader fis;
        private Scanner scanner;
        private Set<String> patternsToSkip = new HashSet<String>();
        private Set<String> stopWordsSet = new HashSet<String>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
            if (conf.getBoolean("wordcount.skip.patterns", false))
            {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs)
                {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName();
                    parseSkipFile(patternsFileName);
                }
            }
        }
        private void parseSkipFile(String fileName)
        {
            try
            {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern;
                while ((pattern = fis.readLine()) != null)
                {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err
                        .println("Caught exception while parsing the cached file '"
                                + StringUtils.stringifyException(ioe));
            }
        }
        // 用来判断字符串是否为数字
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            //获得<key,value>对所属的FileSplit对象（文件分片）
            split = (FileSplit) context.getInputSplit();
            //文件名
            String fileName = split.getPath().getName();
            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
            for (String pattern : patternsToSkip)
            {
                line = line.replaceAll(pattern, " ");
            }
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
            StringTokenizer itr = new StringTokenizer(line);
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
        }
    }
    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>
    {
        private Text info = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
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
        public String listToString(List list, char separator)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < list.size(); i++) {
                sb.append(list.get(i)).append(separator);
            }
            return sb.substring(0, sb.toString().length() - 1);
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
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
            List<Map.Entry<String,Integer>> list = new ArrayList<>(map.entrySet());  //将map的entryset放入list集合
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
    public static void main(String[] args)
    {
        try
        {
            Configuration conf = new Configuration();
            conf.set("mapred.textoutputformat.ignoreseparator", "true");
            conf.set("mapred.textoutputformat.separator", ":");
            GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
            String[] remainingArgs = optionParser.getRemainingArgs();
            Job job = Job.getInstance(conf,"InvertedIndex");
            job.setJarByClass(InvertedIndex.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setCombinerClass(InvertedIndexCombiner.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            List<String> otherArgs = new ArrayList<String>();
            for (int i = 0; i < remainingArgs.length; ++i)
            {
                if ("-skip".equals(remainingArgs[i]))
                {
                    for (int k = i+1; k < remainingArgs.length; ++k)
                    {
                        job.addCacheFile(new Path(remainingArgs[k]).toUri());
                        job.getConfiguration().setBoolean("wordcount.skip.patterns",true);
                    }
                }
                else
                {
                    otherArgs.add(remainingArgs[i]);
                }
            }
            FileInputFormat.addInputPath(job, new Path("shak-txt"));
            FileOutputFormat.setOutputPath(job, new Path("output"));//先将词频统计任务的输出结果写到临时目录中, 下一个排序任务以临时目录为输入目录
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IllegalStateException | IllegalArgumentException | ClassNotFoundException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

