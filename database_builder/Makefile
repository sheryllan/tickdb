TARGET = line2msg
 
$(TARGET).o: $(TARGET).cpp
	c++ -O3 -std=c++11 -I/usr/local/include/python3.4m -I/usr/local/include -fPIC -shared -Wl,--export-dynamic -lboost_python3 -L/usr/local/lib/ -lpython3 -o $(TARGET).so $(TARGET).cpp
