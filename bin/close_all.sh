#!/bin/bash

# 定义一个包含多个端口的数组
ports=(1735 1736 1737 1738 1739 1740 1741 1742 1743 1744 1745 1746 1747 1748 1749 1750 1751 1752 1753 1754 1755 1756 1757 1758 1759)

# 循环遍历数组中的每个端口
for port in "${ports[@]}"; do
  # 获取占用端口的进程ID
  pid=$(sudo lsof -t -i:$port)
  
  # 如果找到进程ID，则终止该进程
  if [ ! -z "$pid" ]; then
    echo "Killing process $pid on port $port"
    sudo kill -9 $pid
  else
    echo "No process found on port $port"
  fi
done

