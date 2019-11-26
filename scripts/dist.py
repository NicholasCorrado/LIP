import random



"""
	To compute the n choose k iteratively

"""
def c(n, k):
	if (k == 0):
		return 1
	if (k == n):
		return 1
	ret = n / k
	for i in range(1, k):
		print(ret)
		ret *= (n - i) / (k - i)
	return int(ret)




"""	
	The distribution class
		groundSet - list of element from the universe
		dist - a distribution over the element
		accuDist - the accumulative distribution

"""

class Distribution:
	groundSet = []
	dist = []
	accuDist = []

	"""
		The user can throw in a random set of numbers for distribution
		and call this method to normalize it to be a distribution
	"""
	def normalize(self):
		s = sum(self.dist)
		self.dist = [val/s for val in self.dist]


	"""
		Given a true normalized distribution, compute the accumulative 
		distribution
	"""
	def computeAccu(self):
		n = len(self.groundSet)

		self.accuDist = [0 for i in range(n+1)]

		for i in range(1, n+1):
			self.accuDist[i] = self.accuDist[i-1] + self.dist[i-1]

	"""
		Given a coin flip value in [0, 1], return the corresponding 
		element in the universe 
	"""
	def GetIndex(self, coin):
		lo = 0
		hi = len(self.groundSet)

		while lo <= hi:
			mid = (lo + hi) // 2

			if self.accuDist[mid] <= coin and coin < self.accuDist[mid+1]:
				return mid

			elif self.accuDist[mid] > coin:
				hi = mid - 1
			elif self.accuDist[mid+1] <= coin:
				lo = mid + 1

		return 0

	"""
		Return a random sample from the distribution
	"""
	def sample(self, i = 0, n = 0):
		coin = 1
		if (i != 0 or n != 0):
			coin = i / n
		else:
			coin = random.random()
		index = self.GetIndex(coin)
		return self.groundSet[index]

	




class BinomialDistribution(Distribution):


	def __init__(self, groundSet, p):
		self.groundSet = groundSet

		n = len(groundSet)
		self.dist = [c(n-1,i)*(p**(i)) * ((1-p)**(n-1-i)) for i in range(n)]
		
		self.normalize()
		self.computeAccu()



class UniformDistribution(Distribution):
	
	def __init__(self, groundSet):
		self.groundSet = groundSet
		self.dist = [1 for i in range(len(groundSet))]

		self.normalize()
		self.computeAccu()

	

class RandomDistribution(Distribution):

	
	def __init__(self, groundSet):

		self.groundSet = groundSet
		n = len(groundSet)

		self.dist = [random.random() for i in range(n)]

		self.normalize()
		self.computeAccu()


class SquareDistribution(Distribution):

	def __init__(self, groundSet):
		self.groundSet = groundSet

		n = len(groundSet)

		self.dist = [0 for i in range(n)]

		for i in range(n//2):
			self.dist[2*i] = 1

		self.normalize()
		self.computeAccu()



class SineDistribution(Distribution):

	def __init__(self, groundSet):
		self.groundSet = groundSet

		n = len(groundSet)
		import math
		self.dist = [math.sin(i * 2 * math.pi / n) + 1 for i in range(n)]

		self.normalize()
		self.computeAccu()



def test():
	
	gs = [i for i in range(10)]

	D = BinomialDistribution(gs, 0.1)

	count = {}
	num = 10000
	for i in range(num):
		d = D.sample()
		if d not in count:
			count[d] = 1
		else:
			count[d] += 1
	for key in count:
		count[key] = count[key] / num
		print(key, abs(count[key] - D.dist[key]))

def main():

	D = SineDistribution([i for i in range(10)])

	for i in range(1):
		d = D.sample(i,1)
		print(i,d)
if __name__ == "__main__":
	main()