## 一个关于hadoop的小例子

使用Hadoop统计file文件夹中各个字母出现的词频，file文件夹中每一行是一个随机的字母，文件的行数也是随机的。

这是使用Hadoop Streaming运行的。Hadoop Streaming是一个框架，可以使用其他语言或者命令操作Hadoop进行计算。

总体来说Hadoop Map-Reduce分为3个步骤。其中1,3中的逻辑需要自己写逻辑完成，2的过程是由Hadoop框架自己完成的：

1. `Map` 对从给定的数据路径文件中读取数据进行并行的第一步运算，返回结果需要被交给下一步`Reduce`进行处理。它被视为key1:value1键值对，key和value的划分是根据由`\t`划分出来的域，通过参数`-jobconf stream.num.map.output.key.fields=n`指定域的数量来进行划分的。

2. `Map` 的结果会根据`key`的大小顺序进行排序。排序默认是使用全部的`key`，也可以通过`-jobconf num.key.fields.for.partition=m`来指定`key`中的域数来进行排序，为了方便叙述，把这种`key`的子集也称为`Key`。相同的`key`会被归类到同一个`Reduce`中。`Reduce`可以形象的理解为接收`Map`结果的桶，拥有相同`Key`的`Map`结果一定会在同一个`Recude`桶中，但是一个`Reduce`桶可以盛放不同的`Map`的`Key`。`Reduce`的个数可以通过参数`-jobconf mapred.reduce.tasks=k`来指定。

3. `Reduce` 将数据并行的进行汇总运算之后，输出至给定的结果路径文件中，最后结果的个数与指定的`Reduce`的数量是相同的。