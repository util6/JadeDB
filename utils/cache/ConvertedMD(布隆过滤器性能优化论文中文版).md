# 构建更好的 Bloom Filter

迈克尔·米岑马赫 亚当·基尔施*

哈佛大学工程与应用科学部 {kirsch,michaelm}@eecs.harvard.教育

## 概述

哈希文献中的一种技术是使用两个哈希函数 $h_{1}(x)$ 和 $h_{2}(x)$ 来模拟 $g_{i}(x)=h_{1}(x)+ih_{2}(x)$ 形式的其他哈希函数。我们证明了这种技术可以有效地应用于 Bloom 过滤器和相关数据结构。具体来说,只需要两个哈希函数即可有效实现 Bloom 过滤器,而不会在渐近误报概率中造成任何损失。这导致更少的计算,并且在实践中可能减少对随机性的需求。

## 1.  介绍

Bloom 过滤器是一种简单的节省空间的随机数据结构,用于表示一个集合以支持成员资格查询。尽管 Bloom 筛选器允许误报,但节省的空间通常超过此缺点。布隆过滤器及其许多变体已被证明对许多应用程序越来越重要(例如,参见 [3])。对于那些不熟悉数据结构的人,我们将在下面的第 2 节中对其进行回顾。在本文中,我们表明应用哈希文献中的标准技术可以

显著简化 Bloom 过滤器的实现。这个想法是这样的:两个哈希函数 $h_{1}(x)$ 和 $h_{2}(x)$ 可以模拟两个以上的哈希函数,形式为 $g_{i}(x)=h_{1}(x)+$ $ih_{2}(x)$ 。(例如,参见 Knuth 对使用双重哈希的开放寻址的讨论 [10]。在我们的上下文中,$i$ 的范围从 O 到某个数字 $k-1$ 以给出 $k$ 个哈希函数,并且哈希值取相关哈希表大小的模数。我们证明了这种技术可以有效地应用于 Bloom 过滤器和相关数据结构。具体来说,只需要两个哈希函数即可有效实现 aBloom 过滤器,而不会增加渐近误报概率。这导致更少的计算,并且在实践中可能减少对随机性的需求。这种改进在 Dillinger 和 Manolios [5, 6] 的工作中得到了实证发现;在这里,我们提供了对该技术的完整理论分析在回顾了 Bloom filter 数据结构之后,我们从一个具体的例子开始,重点

在一个有用但有点理想化的 Bloom filter 结构上,它提供了主要的见解。然后,我们转到一个更通用的设置,该设置涵盖了实践中可能出现的几个问题。例如,当哈希表的大小是 2 的幂而不是素数时。最后,我们通过展示如何使用它来减少 [4] 的 Count-Min 草图所需的哈希函数数量,展示了这种方法在简单的 Bloom 过滤器之外的实用性

------------------------------------------------------------------

##  2.  标准 Bloom 过滤器

我们首先根据调查的介绍回顾 Bloom 过滤器的基础知识 [3],我们参考该调查了解更多详细信息。一个布隆滤波器,用于表示来自一个大型宇宙 $U$ 的一组 $S=\{x_{1},x_{2},\ldots,x_{n}\}$ 的 $7L$ 个元素,由一个 7712 位的数组组成,最初都设置为 0 。过滤器使用 $k$ 个独立的哈希函数 $h_{1},\ldots,h_{k}$,范围为 $\{1,\ldots,m\}$ ,其中假设这些哈希函数将宇宙中的每个元素映射到在该范围内均匀的随机数。虽然哈希函数的随机性显然是一个乐观的假设,但它在实践中似乎是合适的 [8, 13]。对于每个元素 $x\in S$ ,位 $h_{i}(x)$ 设置为 $1\leq i\leq k$ 的 1 。(一个位置可以多次设置为 1。为了检查项目 $y$ 是否在 S 中,我们检查是否所有 $h_{i}(y)$ 都设置为 1。如果不是,那么显然 $y$ 不是 S 的成员。如果所有 $h_{i}(y)$ 都设置为 1 ,我们假设 $y$ 在 S 中,因此布隆过滤器可能会产生误报。不在集合中的元素出现假阳性的概率,或假阳性概率

可以以简单的方式估计,因为我们假设哈希函数是完全随机的。在将 S 的所有元素哈希到 Bloom 过滤器中后,特定位仍为 0 的概率为



$$p'=\left(1-\frac{1}{m}\right)^{kn}\approx\mathrm{e}^{-kn/m}.$$



在本节中,为了方便起见,我们一般使用近似值 $p=\mathrm{e}^{-kn/m}$ 代替 $p^{\prime}$ 如果 $\mu$ 是表中所有 $7L$ 个元素都插入后 0 位的比例,那么条件

在 $\mu$ 上,误报的概率为



$$(1-\rho)^k\approx(1-p')^k\approx(1-p)^k=\left(1-\mathrm{e}^{-kn/m}\right)^k.$$





这些近似值是由于 $\mathbf{E}[\rho]=p^{\prime}$ ,而 $\mu$ 可以使用标准技术显示高度集中在 $p^{\prime}$ 附近。很容易证明,当 $k=\ln2\cdot(m/n)$ 时,表达式 



$\left(1-\mathrm{e}^{-kn/m}\right)^k$



被最小化,给出假阳性概率 $f$ 为



$$f=\left(1-\mathrm e^{-kn/m}\right)^k=(1/2)^k\approx(0.6185)^{m/n}.$$



在实践中,$k$ 必须是一个整数,并且可能首选较小的、次优的 $k$,因为这减少了必须计算的哈希函数的数量。此分析为我们提供了单个项目 $z\notin S$ 给出 false 的概率

阳性。我们想做一个更广泛的声明,事实上这给出了假阳性率。也就是说,如果我们选择大量不在 $S$ 中的不同元素,则产生假阳性的元素比例约为 $f $ 。但这个结果紧接着是因为 $\mu$ 高度集中在 $p^{\prime}$ 附近,因此,假阳性概率有时被称为假阳性率。在继续之前,我们注意到有时 Bloom 过滤器的描述略有不同,其中

每个哈希函数都有一个 $m/k$ 个连续位位置的不相交范围,而不是一个 7712 位的共享数组。重复上面的分析,我们发现在这种情况下,特定位为 0 的概率为 0



$$\left(1-\dfrac{k}{m}\right)^n\approx\mathrm e^{-kn/m}.$$



渐近地,性能与原始方案相同,尽管因为对于 $k\geq1$



$$\left(1-\frac{k}{m}\right)^n\leq\left(1-\frac{1}{m}\right)^{kn},$$



此修改永远不会降低假阳性概率

------------------------------------------------------------------

## 3. 使用两个哈希函数的简单构造。

作为一个具有指导意义的示例案例,我们考虑了引言中描述的一般技术的以下具体应用。我们设计了一个 Bloom 过滤器,它使用 $k$ 个哈希函数,每个函数的范围为 $\{0,1,2,\ldots,p-1\}$ 作为素数 $P $。我们的哈希表由 $m=kp$ 位组成;每个哈希函数在过滤器中都分配了一个 $P$ 位的不相交子数组,我们将其视为编号 $\{0,1,2,\ldots,p-1\}$ 。我们的 $k$ 哈希函数将是这样的

$$g_i(x)=h_1(x)+ih_2(x)\bmod p,$$

其中 $h_{1}(x)$ 和 $h_{2}(x)$ 是宇宙中两个独立的、统一的随机哈希函数,范围为 $\{0,1,2,\ldots,p-1\}$ ,在整个过程中,我们假设 $i$ 的范围从 0 到 $k-1$

在此设置中,对于任意两个元素 $JL $ 和 $y$ ,恰好出现以下三种情况之一:

1. $g_{i}(x)\neq g_{i}(y)$ 对于所有 $\dot{\hat{x}}$ ;或

2. $g_{i}(x)=g_{i}(y)$ 恰好是一个 $\dot{i}$ ;或

3. 所有 $\dot{i}$ 的 $g_{i}(x)=g_{i}(y)$

也就是说,如果 $g_{i}(x)=g_{i}(y)$ 至少有两个值 $i ,那么很明显我们必须有 $h_{1}(x)=h_{1}(y)$ 和 $h_{2}(x)=h_{2}(y)$ ,所以所有的哈希值都是相同的。正是这个属性暗示了分析,并使它成为一个有启发性的例子;在第 4 节中,我们考虑了可能发生其他重要冲突的更一般情况。

第一步,我们考虑一组 $S=\{x_{1},x_{2},\ldots,x_{n}\}$ $U$中的 7t 个元素和 $z\notin S$ 中的元素,并计算 Z 产生假阳性的概率。假阳性对应于事件 $\mathcal{F}$,对于每个 $i$,(至少)有一个 $j$,使得 $g_{i}( z)$ = $g_{i}( x_{j})$ 显然,发生这种情况的一种方式是,如果 $h_{1}(x_{j})=h_{1}(z)$ 并且 $h_{2}(x_{j})=h_{2}(z)$ 对于某些 $j$ 。此事件 $\xi$ 的概率为

$$\mathbf{Pr}(\mathcal{E})=1-\left(1-\frac{1}{p^2}\right)^n=1-\left(1-\frac{k^2}{m^2}\right)^n.$$

请注意,当某个常数 $C$ 的 $k=cm/n$ 时,按照布隆滤波器的标准,我们有 $\mathbf{Pr}(\mathcal{E})=o(1)$ 。现在因为



$$\begin{aligned}\mathbf{Pr}(\mathcal{F})&=\mathbf{Pr}(\mathcal{F}\mid\mathcal{E})\mathbf{Pr}(\mathcal{E})+\mathbf{Pr}(\mathcal{F}\mid\neg\mathcal{E})\mathbf{Pr}(\neg\mathcal{E})\\&=\mathbf{Pr}(\mathcal{E})+\mathbf{Pr}(\mathcal{F}\mid\neg\mathcal{E})\mathbf{Pr}(\neg\mathcal{E})\\&=o(1)+\mathbf{Pr}(\mathcal{F}\mid\neg\mathcal{E})(1-o(1)),\end{aligned}$$



考虑 $\mathbf{Pr}(\mathcal{F}\mid\neg\mathcal{E})$ 即可获得渐近假阳性概率,当 $m/n$ 和 $k$ 为常数时,该概率为常数

以 $\neg{\mathcal E}$ 和 $(h_{1}(z),h_{2}(z))$ 为条件,对 $(h_{1}(x_{j}),h_{2}(x_{j}))$ 均匀分布在 $V=\{0,\ldots,p-1\}^{2}-(h_{1}(z),h_{2}(z))$ 的 $p^{2}-1$ 值上。其中,对于每个 $i^{*}\in\{0,\ldots,k-1\}$ 中的 $p-1$ 对

$$V'=\{(a,b)\in V\::\:a\equiv i^*(h_2(z)-b)+h_1(z)\bmod p,\:b\not\equiv h_2(z)\bmod p\}$$

是这样的值,如果 $(h_{1}(x_{j}),h_{2}(x_{j}))\in V^{\prime}$ ,则 $\ddot{\boldsymbol{i}}^{*}$ 是 $i$ 的唯一值,使得 $g_{i}(x_{j})=$ $g_{i}(z)$。因此,我们可以将条件概率视为 balls-and-bins 问题的变体。有 $7l$ 的球和 $k$ 的桶。概率为 $k(p-1)/(p^{2}-1)=k/(p+1)$ 一个球落在箱子里,剩下的概率被丢弃;当球落入 bin 时,bin 会将其

Lands In 是随机均匀选择的。所有 bin 都至少有一个球的概率是多少？

这可以用多种方式表达。首先,我们可能还记得,从一组大小为 $U.$ 到一组大小为 $b$ 的射门数是由 $b 给出的！S(a,b)$ ,其中 $S(a,b)$ 是指第二类的斯特林数。然后我们直接有

$$\mathbf{Pr}(\mathcal{F}\mid\neg\mathcal{E})=\sum\limits_{a=k}^n{n\choose a}\left(\dfrac{k}{p+1}\right)^a\left(1-\dfrac{k}{p+1}\right)^{n-a}\dfrac{k！S(a,k)}{k^a}.$$

可以将此表达式的极限设为 $Tl\rightarrow\mathbf{X}$(例如,参见 [2] 的讨论)。

或者,我们可能会注意到,对于标准的 Bloom filter,我们也有类似的问题。假设元素 $z\notin S$ 的每个 $k$ 哈希值都是不同的(这种情况发生的概率很高),在这种情况下,有 $nk$ 个球(每个项目的每个哈希一个),每个球都有 $k/m$ 的概率落入箱子,这对应于 $z$ 的哈希值。很明显,在限制中为 711。并且 $7l$ 变大并且 $k$ 保持为固定常数,那么在这两种情况下,落入 bin 的球数的分布都会收敛到相同的分布,因此误报的概率会收敛到
$$f=\begin{pmatrix}1-\mathrm{e}^{-kn/m}\end{pmatrix}^k$$

在这两种情况下。正如我们已经说过的,第 4 节将给出一个更正式和一般的论点。现在,就像第 2 节一样,我们必须论证 $f$ 不仅仅是渐近假阳性概率

但它也像假阳性率。与标准 Bloom 滤波器的情况类似,这归结为一个集中论点。一旦对集合 $S$ 进行哈希处理,就会有一个集合

$$B=\{(b_1,b_2):h_1(z)=b_1\text{and}h_2(z)=b_2\text{implies}z\text{给出假阳性}\}.$$

以 $\left|B\right|$ 中,$U-S$ 中任何元素出现假阳性的概率为 $|B|/p^{2}$ ,并且这些事件是独立的。如果我们显示 $\left|B\right|$ 集中在它的期望附近,那么很容易得出,一组不在 S 中的不同元素中的假阳性分数集中在 $f$ 附近

一个简单的 Doob 马丁格尔论点就足够了。$S$ 的每个哈希元素可以在任一方向上更改 $B$ 中的对数最多 $kp$。从 [12, Section 12.5] 中,对于任何 $\epsilon>0$
$$\mathbf{Pr}(|B-\mathbf{E}[B]|\geq\epsilon p^2)\leq2\exp\left[\frac{-2\epsilon^2p^2}{nk^2}\right].$$

现在很容易得出所需的结论。我们将细节推迟到第 7 节,在那里我们对更一般的结果提供更严格的证明。

## 4.  一般框架

在本节中,我们介绍了一个用于分析非标准 Bloom filter 方案的通用框架,例如 Section 3 中研究的框架。我们表明,在非常广泛的条件下,方案的渐近假阳性概率与标准 Bloom 滤波器相同。在深入研究细节之前,我们必须引入一些符号。对于任何整数 $\ell$ ,我们定义

集合 $[\ell]=\{0,1,\ldots,\ell-1\}$ (请注意,这个定义有点不标准)。对于随机变量 $X ,我们用 $\operatorname{Supp}(X)$ 表示 $X$ 的支持,如果 $Y$ 是另一个随机变量,那么 $X\sim Y$ 表示 $X$ 和 $Y$ 具有相同的分布。此外,我们使用 $Po(\lambda)$ 来表示参数为 $\lambda$ 的泊松分布

我们还需要一些关于多集合的符号。对于多集合 $M $ ,我们使用 $\left|M\right|$ 表示 $M$ 的不同元素的数量,$\|M\|$ 表示具有多重性的 $M$ 的元素数。对于两个多集 $M$ 和 $M^{\prime}$ ,我们将 $M\cap M^{\prime}$ 和 $M\cup M^{\prime}$ 分别定义为多集$M的交集和并集。此外,在滥用标准表示法的情况下,我们将语句 $i,i\in M$ 定义为 $i$ 是 $M$ 的重数至少 2 的元素。现在,我们已准备好定义框架。与前面部分一样,$U$ 表示宇宙

的项数和 $S\subseteq U$ 表示 Bloom 筛选器将回答其成员资格查询的 $TL$ 项集。我们将一个方案定义为一种将哈希位置分配给 $U$ 的每个元素的方法。更正式地说,方案由离散随机变量 $\{H(u):u\in U\}$ 的联合分布指定(由 $7t$ 隐式参数化),其中对于 $u\in U$,$H(u)$ 表示方案分配给 $u.$ 的多组哈希位置。我们不需要为每个 $7L$ 的值定义一个方案,但我们坚持要为 $Tt$ 的无限多个值定义它,这样我们就可以把极限当作 $Tl\rightarrow\mathbf{x}$ 。例如,对于第 3 节中讨论的方案类别,我们认为常数 $k$ 和 $C$ 是固定的,以给出一个特定的方案,该方案仅针对 $Tl.$ 的值定义,使得 $p\stackrel{\mathrm{def}}{=}m/k$ 是一个素数,其中 $m\stackrel{\mathrm{def}}{=}cn$ 由于有无限多个素数,这个方案的渐近行为 $TL\rightarrow0$ 是明确定义的,与第 3 节中讨论的完全相同,其中我们让 TIl 是一个自由参数,并分析了 $m,m\to\infty$ 的行为,条件是 $m/n$ 和 $k$ 是固定常数,$m/k$ 是素数。在定义了方案的概念之后,我们现在可以用

new 表示法(所有这些都由 $7l$ 隐式参数化)。我们将 $H$ 定义为方案可以分配的所有哈希位置的集合(正式地,$H$ 是在某些多集合中应用的元素集,以支持 $H(u)$ ,对于某些 $u\in U$ )。对于 $x\in S$ 和 $z\in U-S$ ,定义 $C(x,z)=H(x)\cap H(z)$ 为 $L$ 与 $Z$ 的多组哈希冲突。我们让 $\mathcal{F}(z)$ 表示 $z\in U-S$ 的假阳性事件,当 $Z$ 的每个哈希位置也是某些 $x\in S$ 的哈希位置时,就会发生该事件在我们考虑的方案中,$\{ H( u) :$ $u\in U\}$ 将始终是独立且相同的

分散式。在这种情况下,$\mathbf{Pr}(\mathcal{F}(z))$ 对于所有 $z\in U-S$ 都是相同的,$\{ C( x, z) :$ $x\in S\}$ 的联合分布也是相同的。因此,为了简化符号,我们可以在 U-S$ 中固定一个任意的$z\,简单地使用 $\Pr(\mathcal{F})$ 而不是 $\mathbf{Pr}(\mathcal{F}(z))$ 来表示假阳性概率,我们可以使用 $\{ C( x) :$ $x\in S\}$ 而不是 $\{ C( x, z) :$ $x\in S\}$ 来表示 S 元素与 $z$ 的多组哈希冲突的联合概率分布本节的主要技术成果是以下关键定理,它是一个形式化

以及第 3 节中给出的论点的推广,以表明那里分析的方案的渐近假阳性概率与具有相同参数的标准布隆滤波器相同。

定理 4.1.Fir a scheme.假设存在常数 $\lambda$ 和 $k$,使得:

1. $\{H(u):u\in U\}$ 是独立的,分布相同

2.对于 $u\in U$ , $\|H(u)\|=k$

3.$x $

$$\mathbf{Pr}(\|C(x)\|=i)=\left\{\begin{array}{cc}1-\frac{\lambda}{n}+o(1/n)&i=0\\\frac{\lambda}{n}+o(1/n)&i=1\\o(1/n)&i>1\end{array}\right..$$

4.$x $

$$\max\limits_{i\in H}\left|\mathbf{Pr}(i\in C(x)\mid\|C(x)\|=1,\:i\in H(z))-\frac{1}{k}\right|=o(1)\quad as\:n\to\infty.$$

然后

$$\lim\limits_{n\to\infty}\mathbf{Pr}(\mathcal{F})=\left(1-\mathrm{e}^{-\lambda/k}\right)^k.$$

证明。为了便于解释,我们为 $H(z)$ 的每个元素分配一个 $[k]$ 中的唯一数字(将同一哈希位置的多个实例视为不同的元素)。更正式地说,我们为每个多集 $M\subseteq H$ 定义一个从 $M$ 到 $[k]$ 的任意双射 $f_{M}$,其中 $\|M\|=k$ (其中 $f_{M}$ 将 $M$ 中相同哈希位置的多个实例视为不同的元素),并根据 $f_{H(z)}$ 标记 $H(z)$ 的元素。这个约定允许我们通过数字 $i\in[k]$ 来识别 $H(z)$ 的元素,而不是哈希位置 $i\in H$ 对于 $i\in[k]$ 和 $x\in S$ ,如果 $i\in C(x)$ 定义 $X_{i}(x)=1$,否则定义 0,并定义。$X_i\stackrel{\mathrm{def}}{=}$

$\sum_{x\in S}X_{i}(x)$ .请注意,$i\in C(x)$ 是对符号的滥用;我们真正的意思是 $f_{H(z)}^{-1}(i)\in C(x)$,尽管我们将继续使用前者,因为它要麻烦得多我们表明$X^{n}\stackrel{\mathrm{det}}{=}(X_{0},\ldots,X_{k-1})$ 在分布中收敛到一个向量 $P\stackrel{\mathrm{def}}{=}(P_{0},\ldots,P_{k-1})$

的 $k$ 个参数为 $\lambda/k$ 的独立泊松随机变量,如 $Tl\rightarrow\mathbf{x}$ 。为此,我们使用了 moment 生成函数。对于随机变量 $R$ ,$R.$ 的矩生成函数由 $M_{R}( t) \overset {\mathrm{def}}{\operatorname* { = } }\mathbf{E} [ \exp ( tR) ]$ 定义。我们表明,对于任何$t_{0},\ldots,t_{k}$

$$\lim_{n\to\infty}M_{\sum_{i=0}^{k-1}t_{i}X_{i}}(t_{k})=M_{\sum_{i=0}^{k-1}t_{i}P_{i}}(t_{k}),$$

这已经足够了 [1, 定理 29.4 和第 390 页],因为

$$\begin{aligned}M_{\sum_{i=0}^{k-1}t_{i}P_{i}}(t_{k})&=\mathbf{E}\left[\mathrm{e}^{t_{k}\sum_{i\in[k]}t_{i}P_{i}}\right]\\&=\prod_{i\in k}\mathbf{E}\left[\mathrm{e}^{t_{k}t_{i}\mathrm{Po}(\lambda/k)}\right]\\&=\prod_{i\in k}\sum_{j=0}^{\infty}\mathrm{e}^{-\lambda/k}\frac{\lambda^{j}{k^{j}j！}\mathrm{e}^{t_{k}t_{i}j}\\&=\prod_{i\in k}\mathrm{e}^{\frac{\lambda}{k}\left(\mathrm{e}^{t_{k}t_{i}}-1\right)}\\&=\mathrm{e}^{\frac{\lambda}{k}\left(\sum_{i\in k}\mathrm{e}^{t_{k}t_{i}}-1\right)}<\infty,\end{aligned}$$

其中第一步只是矩生成函数的定义,第二步是从 $t_{i}P_{i}(\lambda_{k})$ 的独立性开始,第三步只是泊松分布的定义,第四步是从 $\mathrm{e}^{x}$ 的泰勒级数开始,第五步是显而易见的。

接下来,我们写道

$$\begin{aligned}
&M_{\sum_{i\in[k]}t_{i}X_{i}}(t_{k}) \\
&=M_{\sum_{i\in[k]}t_{i}\sum_{x\in S}X_{i}(x)}(t_{k}) \\
&=M_{\sum_{x\in S}\sum_{i\in[k]}t_iX_i(x)}(t_k) \\
&=\begin{pmatrix}M_{\sum_{i\in[k]}t_iX_i(x)}(t_k)\end{pmatrix}^n \\
&= \left( \mathrm{Pr}(\|C(x)\|=0)\right.\\
&+\sum_{j=1}^{k}\Pr(\|C(x)\|=j)\sum_{T\subseteq[k]:|T|=j}\Pr(C(x)=f_{H(z)}^{-1}(T)\mid\|C(x)\|=j)\mathrm{e}^{t_{k}\sum_{i\in T}t_{i}}\Bigg) \\
&=\left(1-\frac{\lambda}{n}+\frac{\lambda}{n}\sum_{i\in[k]}\Pr(i\in C(x)\mid\|C(x)\|=1)\mathrm{e}^{t_kt_i}+o(1/n)\right)^n \\
&=\left(1-\frac{\lambda}{n}+\frac{\lambda}{n}\sum_{i\in[k]}\left(\frac{1}{k}+o(1)\right)\mathrm{e}^{t_{k}t_{i}}+o(1/n)\right)^{n} \\
&= \left(1-\frac{\lambda}{n}+\frac{\lambda\sum_{i\in[k]}\mathrm{e}^{t_{k}t_{i}}}{kn}+o(1/n)\right)^{n} \\
&\to\mathrm{e}^{-\lambda+\frac{\lambda}{k}\sum_{i\in[k]}\mathrm{e}^{t_{k}t_{i}}}\quad\mathrm{as} n\to\infty \\
&= \mathrm{e}^{\frac{\lambda}{k}\left(\sum_{i\in[k]}\left(\mathrm{e}^{t_{k}t_{i}}-1\right)\right)} \\
&= M_{\sum_{i\in[k]}t_{i}\mathrm{Po}_{i}(\lambda_{k})}(t_{k}).
\end{aligned}$$

前两个步骤是显而易见的。第三步是从 $H(x)$ 是独立的这一事实开始的

dent 和以 $H(z)$ 为条件的相同分布(对于 $x\in S$ ),所以 $\sum_{i\in[k]}t_{i}X_{i}(x)$ 也是,因为每个都是相应 $H(x)$ 的函数。第四步来自力矩生成函数的定义。第五步和第六步遵循对 $C(x)$ 分布的假设(在第六步中,$i\in H(z)$ 的条件隐含在我们的约定中,它将 $[k]$ 中的整数与 $H(z)$ 的元素相关联)。第七步、第八步和第九步是显而易见的,第十步遵循前面的计算。现在修复一些双射 $g$ : $\mathbb{Z} _{\geq 0}^{k}$ $\to$ $\mathbb{Z} _{\geq 0}$ ,并定义 $h:\mathbb{Z}_{\geq0}\to\{0,1\}:h(x)=1$ if 和

仅当 $g^{-1}(x)$ 的每个坐标都大于 0 时。由于 $\{X^{n}\}$ 在分布中收敛到 $P$,因此 $\{g(X^{n})\}$ 在分布中收敛到 $g(P)$,因为 $y$ 是一个双射,而 $X^{n}$ 和 $P$ 具有离散分布。Skorohod 表示定理 [1, 定理 25.6] 现在意味着存在一些概率空间,可以在其中定义随机变量 $\{Y_{n}\}$ 和 $P^{\prime}$ ,其中 $Y_{n}\sim g(X^{n})$ 和 $P^{\prime}\sim g(P)$ ,以及 $\{Y_{n}\}$ 几乎可以肯定地收敛到 $P^{\prime}$。当然,由于 $Y_{n}$ 只取整数值,因此每当 $\{Y_{n}\}$ 收敛到 $P^{\prime}$ 时,必须有一些 $7l_{0}$,使得 $Y_{n_{0}}=Y_{n_{1}}=P^{\prime}$ 对于任何$n_{1}>n_{0}$ ,因此 $\{h(Y_{n})\}$ 很容易收敛到 $h(P^{\prime})$ 。因此,$\{h(Y_{n})\}$ 收敛为

$h(P^{\prime})$ 几乎可以肯定,所以

$$\begin{aligned}\mathbf{Pr}(\mathcal{F})&=\mathbf{Pr}(\forall i\in[k],X_{i}>0)\\&=\mathbf{E}[h(g(X^{n}))]\\&=\mathbf{E}[h(Y_{n})]\\&\to\mathbf{E}[h(P^{\prime})]]\quad\mathrm{as}\:n\to\infty\\&=\mathbf{Pr}(\mathrm{Po}(\lambda/k)>0)^{k}\\&=\left(1-\mathrm{e}^{-\lambda/k}\right)^{k},\end{aligned}$$

其中第四步是唯一的非平凡步骤,它遵循[1,定理 5.4]

事实证明,定理 4.1 的条件在许多情况下可以很容易地验证。

引理 4.1.Fir a scheme.假设存在常数 $\lambda$ 和 $k$,使得

1. $\{H(u):u\in U\}$ 是独立的,分布相同

2.对于 $u\in U$ , $\|H(u)\|=k$

3.对于 $u\in U$

$$\max\limits_{i\in H}\left|\mathbf{Pr}(i\in H(u))-\frac{\lambda}{kn}\right|=o(1/n).$$

4.$u $

$$\max_{i_{1},i_{2}\in H}\mathbf{Pr}(i_{1},i_{2}\in H(u))=o(1/n).$$

5.所有可能的哈希位置集合 $H$ 满足 $|H|=O(n)$

那么定理 4.1 的条件成立($\lambda$ 的值相同),因此结论也是如此。

备注。回想一下,在我们的符号下,当且仅当 $\dot{i}$ 是重数至少 2 的 $H(u)$ 的元素时,语句 $i,i\in H(u)$ 才成立。

证明。我们采用定理 4.1 证明中引入的约定,其中 $H(z)$ 的元素由 $[k]$ 中的整数标识

定理 4.1 的前两个条件很容易满足。对于第三个条件,观察对于任何 $j\in\{2,\ldots,k\}$ 和 $x\in S$

$$\begin{aligned}\mathbf{Pr}(\|C(x)\|=j)&\leq\mathbf{Pr}(\|C(x)\|>1)\\&=\mathbf{Pr}(\exists i_{1}\leq i_{2}\in[k]:i_{1},i_{2}\in H(x)\mathrm{~or~}\exists i\in H:i\in H(x),i,i\in H(z))\\&\leq\sum_{i_{1}\leq i_{2}\in[k]}\mathbf{Pr}(i_{1},i_{2}\in H(x))+\sum_{i\in H}\mathbf{Pr}(i\in H(x))\mathbf{Pr}(i,i\in H(z))\\&\leq k^{2}o(1/n)+|H|\left(\frac{\lambda}{kn}+o(1/n)\right)o(1/n)\\&=o(1/n)+|H|o(1/n^{2})\\&=o(1/n)+O(n)o(1/n^{2})\\&=o(1/n)\end{aligned}$$

和

$$\Pr(\|C(x)\|=1)\leq\Pr(|C(x)|\geq1)\leq\sum_{i\in[k]}\Pr(i\in H(x))\leq k\left({\frac{\lambda}{kn}}+o(1/n)\right)={\frac{\lambda}{n}}+o(1/n)$$

和

$$\begin{aligned}\mathbf{Pr}(\|C(x)\|\geq1)&=\mathbf{Pr}\left(\bigcup_{i\in[k]}i\in H(x)\right)\\&\geq\sum_{i\in[k]}\mathbf{Pr}(i\in H(x))-\sum_{i_{1}<i_{2}\in[k]}\mathbf{Pr}(i_{1},i_{2}\in H(x))\\&\geq k\left(\frac{\lambda}{kn}+o(1/n)\right)-k^{2}o(1/n)\\&=\frac{\lambda}{n}+o(1/n),\end{aligned}$$

所以

$$\begin{aligned}\mathbf{Pr}(\|C(x)\|=1)&=\mathbf{Pr}(\|C(x)\|\geq1)-\mathbf{Pr}(\|C(x)\|>1)\\&\geq\frac{\lambda}{n}+o(1/n)-o(1/n)\\&=\frac{\lambda}{n}+o(1/n).\end{aligned}$$

因此

$$\Pr(\|C(x)\|=1)=\frac{\lambda}{n}+o(1/n),$$

和

$$\begin{aligned}\mathbf{Pr}(\|C(x)\|=0)=1-\sum_{j=1}^k\mathbf{Pr}(\|C(x)\|=j)=1-\frac{\lambda}{n}+o(1/n).\end{aligned}$$

我们现在已经证明,定理 4.1 的第三个条件是满足的

对于第四个条件,我们观察到对于任何 $i\in[k]$ 和 $x\in S$

$$\mathbf{Pr}(i\in C(x),\|C(x)\|=1)\leq\mathbf{Pr}(i\in H(x))=\frac{\lambda}{kn}+o(1/n),$$

和

$$\begin{aligned}\mathbf{Pr}(i\in C(x),\|C(x)\|=1)&=\mathbf{Pr}(i\in H(x))-\mathbf{Pr}(i\in H(x),\|C(x)\|>1)\\&\geq\mathbf{Pr}(i\in H(x))-\mathbf{Pr}(\|C(x)\|>1)\\&=\frac{\lambda}{kn}+o(1/n)-o(1/n),\end{aligned}$$

所以

$$\Pr(i\in C(x),\|C(x)\|=1)=\frac{\lambda}{kn}+o(1/n),$$

这意味着

$$\mathbf{Pr}(i\in C(x)\mid\|C(x)\|=1)={\frac{\mathbf{Pr}(i\in C(x),\|C(x)\|=1)}{\mathbf{Pr}(\|C(x)\|=1)}}={\frac{{\frac{\lambda}{kn}}+o(1/n)}{{\frac{\lambda}{n}}+o(1/n)}}={\frac{1}{k}}+o(1),$$

完成证明($i\in H(z)$的条件再次被约定所暗示,即$\sqcup$将$[k]$的元素与$H(z)$中的哈希位置相关联

##  5.  一些具体方案

我们现在准备分析一些具体的方案。特别是,我们研究了第 3 节中描述的方案的自然泛化,以及 [5, 6] 中介绍的双重哈希和扩展双重哈希方案

在这两种情况下,我们都考虑一个由 $TIl=CTb$ 位数组和 $k$ 哈希函数组成的 Bloom 过滤器,其中 $c>0$ 和 $k\geq1$ 是固定常数。哈希函数的性质取决于所考虑的特定方案

### 5.1分区方案

首先,我们考虑分区方案的类,其中 Bloom 过滤器由一个 7712 位的数组定义,该数组被划分为 $k$ 个 $m^{\prime}=m/k$ 位的不相交数组(我们要求 $711l$ 能被 $k$ 整除),并且一个项目 $u\in U$ 被哈希到位置

$$h_1(u)+ih_2(u)\bmod m'$$

数组 $i$ ,对于 $i\in[k]$ ,其中 $h_{1}$ 和 $h_{2}$ 是具有共域 $[m^{\prime}]$ 的独立完全随机哈希函数。请注意,第 3 节中分析的方案是一个分区方案,其中 $m^{\prime}$ 是素数(因此在第 3 节中用 $P$ 表示)除非另有说明,否则我们进行所有涉及 $h_{1}$ 和 $h_{2}$ 模数 $m^{\prime}$ 的算术运算

我们证明了以下关于分区方案的定理

定理 5.1.对于分区方案,

$$\lim\limits_{n\to\infty}\mathbf{Pr}(\mathcal{F})=\left(1-\mathrm{e}^{-k/c}\right)^{k}.$$

证明。我们将证明 $H(u)$ 满足引理 4.1 的条件,其中 $\lambda=k^{2}/c$ 对于 $i\in[k]$ 和 $u\in U$ ,定义

$$\begin{aligned}g_i(u)&=(i,h_1(u)+ih_2(u))\\H(u)&=(g_i(u)\::\:i\in[k]).\end{aligned}$$

也就是说,$g_{i}(u)$ 是 $u$ 的第 i 个哈希位置,$H(u)$ 是 $your$ 的多组哈希位置。这个表示法显然与引理 4.1 要求的定义一致,因为 $h_{1}$ 和 $h_{2}$ 是独立的和完全随机的,所以前两个条件是微不足道的。这

最后一个条件也是微不足道的,因为有 $TH=CHL$ 可能的哈希位置。对于其余两个条件,请修复 $u\in U$ 。观察到,对于 $(i,r)\in[k]\times[m^{\prime}]$

$$\mathbf{Pr}((i,r)\in H(u))=\mathbf{Pr}(h_{1}(u)=r-ih_{2}(u))=\frac{1}{m^{\prime}}=\frac{k^{2}/c}{kn},$$

对于不同的 $(i_{1},r_{1}),(i_{2},r_{2})\in[k]\times[m^{\prime}]$ ,我们有

$$\begin{aligned}\mathbf{Pr}((i_{1},r_{1}),(i_{2},r_{2})\in H(u))&=\mathbf{Pr}(i_{1}\in H(u))\:\mathbf{Pr}(i_{2}\in H(u)\mid i_{1}\in H(u))\\&=\frac{1}{m^{\prime}}\mathbf{Pr}(h_{1}(u)=r_{2}-i_{2}h_{2}(u)\mid h_{1}(u)=r_{1}-i_{1}h_{2}(u)))\\&=\frac{1}{m^{\prime}}\mathbf{Pr}((i_{1}-i_{2}))h_{2}(u)=r_{1}-r_{2})\\&\leq\frac{1}{m^{\prime}}\cdot\frac{\gcd(|i_{2}-i_{1}|,m^{\prime})}{m^{\prime}}\\&\leq\frac{k}{(m^{\prime})^{2}}\\&=o(1/n)\end{aligned}$$

------------------------------------------------------------------

其中第四步是唯一重要的步骤,它从标准事实得出,对于任何$r,s\in[m]$ ,最多有 $\gcd(r,m)$ 值 $t\in[m]$ 使得 $rt\equiv s\bmod m$ m TIl(例如,参见[9,命题 3.3.1])。最后,由于从方案的定义中可以清楚地看出 $\left|H(u)\right|=k$ 对于所有 $u\in U$ ,我们有任何 $(i,r)\in[k]\times[m^{\prime}]$

$$\mathbf{Pr}((i,r),(i,r)\in H(u))=0.$$

### 5.2 (扩展的) 双重哈希方案

接下来,我们考虑双哈希和扩展双哈希方案的类别,在 [5, 6] 中对它们进行了实证分析。在这些方案中,项目 $u\in U$ 被哈希处理到 location

$$h_1(u)+ih_2(u)+f(i)\bmod m$$

在 $7TL$ 位的数组中,对于 $i\in[k]$ ,其中 $h_{1}$ 和 $h_{2}$ 是具有共域 $[m]$ 的独立完全随机哈希函数,$f:[k]\to[m]$ 是一个任意函数。当 $f(i)\equiv0$ 时,该方案称为双哈希方案。否则,称为 ertended double hashing scheme(带 $f$ )

除非另有说明,否则我们进行所有涉及 $h_{1}$ 和 $h_{2}$ 模数 TIl 的运算 TIl 我们证明了以下关于双重哈希方案的定理。

定理 5.2.对于任何(扩展的)双重哈希方案。

$$\lim\limits_{n\to\infty}\mathbf{Pr}(\mathcal{F})=\left(1-\mathrm{e}^{-k/c}\right)^{k}.$$

备注。结果适用于 $f $ 的任何选择。事实上,$f$ 甚至可以从 $[m]^{[k]}$ 的任意概率分布中得出,只要它是独立于两个随机哈希函数 $h_{1}$ 和 $h_{2}$ 绘制的

证明。我们继续证明该方案满足引理 4.1 的条件(对于 $\lambda=k^{2}/c$ )。由于 $h_{1}$ 和 $h_{2}$ 是独立且完全随机的,因此前两个条件很容易成立。最后一个条件也很微不足道,因为有 $TH=CTL$ 个可能的哈希位置

证明第三个和第四个条件成立需要付出更多的努力。首先,我们需要一些符号。对于 $u\in U$ , $i\in[k]$ ,定义

$$\begin{aligned}g_i(u)&=h_1(u)+ih_2(u)+f(i)\\H(u)&=(g_i(u):\:i\in[k]).\end{aligned}$$

也就是说,$g_{i}(u)$ 是 $u$ 的第 i 个哈希位置,$H(u)$ 是 $u$ 的多组哈希位置。这种表示法显然与 Lemma 4.1 要求的定义一致。修复 $u\in U$ 。对于 $r\in[m]$
$$\begin{aligned}\mathbf{Pr}(\exists j\in[k]:g_j(u)=r)\leq\sum_{j\in[k]}\mathbf{Pr}(h_1(u)=r-jh_2(u)-f(j))=\frac{k}{m}.\end{aligned}$$

此外,对于任何$j_{1},j_{2}\in[k]$ 和 $r_{1},r_{2}\in[m]$

$$\begin{aligned}\mathbf{Pr}(g_{j_{1}}(u)=r_{1}, g_{j_{2}}(u)=r_{2})&=\mathbf{Pr}(g_{j_{1}}(u)=r_{1})\mathbf{Pr}(g_{j_{2}}(u)=r_{2}\mid g_{j_{1}}(u)=r_{1})\\&=\frac{1}{m}\mathbf{Pr}(g_{j_{2}}(u)=r_{2}\mid g_{j_{1}}(u)=r_{1})\\&=\frac{1}{m}\mathbf{Pr}((j_{1}-j_{2})h_{2}(u)=r_{1}-r_{2}+f(j_{2})-f(j_{1}))\\&\leq\frac{1}{m}\cdot\frac{\gcd(|j_{1}-j_{2}|,m)}{m}\\&\leq\frac{1}{m}\cdot\frac{k}{m}\\&=\frac{k}{m^{2}}\\&=o(1/n),\end{aligned}$$

其中第四步是唯一重要的一步,它从标准事实得出,对于任何$r,s\in[m]$ ,最多有 $\gcd(r,m)$ 值 $t\in[m]$,使得 $rt\equiv s$ mod 711。例如,参见 [9, Proposition 3.3.1])。因此,对于 $r\in[m]$

$$\begin{aligned}\mathbf{Pr}(\exists j\in[k]:g_{j}(u)=r)&\geq\sum_{j\in[k]}\mathbf{Pr}(g_{j}(u)=r)-\sum_{j_{1}<j_{2}\in[k]}\mathbf{Pr}(g_{j_{1}}(u)=r,g_{j_{2}}(u)=r)\\&\geq\frac{k}{m}-k^{2}o(1/n)\\&=\frac{k}{m}+o(1/n),\end{aligned}$$

这意味着

$$\mathbf{Pr}(r\in H(u))=\mathbf{Pr}(\exists j\in[k]:g_j(u)=r)=\frac{k}{m}+o(1/n),$$

所以引理 4.1 的第三个条件成立。对于第四个条件,请修复任何 $r_{1},r_{2}\in[m]$ 。然后

$$\mathbf{Pr}(r_{1},r_{2}\in H(u))\leq\sum_{j_{1},j_{2}\in[k]}\mathbf{Pr}(g_{j_{1}}(u)=r_{1},g_{j_{2}}(u)=r_{2})\leq k^{2}o(1/n)=o(1/n),$$

完成校对。




## 6.收敛速率

在前面的部分中,我们确定了一大类非标准 Bloom 过滤器方案,它们与标准 Bloom 过滤器具有相同的渐近假阳性概率。不幸的是,这些结果在空间非常有限的环境中并不是特别引人注目,因为有理由认为定理 4.1 结论的收敛速度可能相当慢。Bloom 过滤器在空间极其有限的应用程序中特别有吸引力(例如,参见 [3]),因为它们提供相当小的错误率,而每个项目只使用少量的恒定位数。因此,考虑到这些应用,我们在定理 4.1 中提供了对收敛速率的详细分析。在继续结果之前,我们介绍一些有用的符号。对于函数 $f(n)$ 和

$g(n)$ ,我们用 $f(n)\sim g(n)$ 来表示 $\operatorname*{lim}_{n\to\infty}f(n)/g(n)=1$ 。同样,我们使用 $f(n)\lesssim g(n)$ 来表示 lim: $\operatorname*{sup}_{n\to\infty}f(n)/g(n)\leq1$ 和 $f(n)\gtrsim g(n)$ 来表示 $\operatorname*{lim}\operatorname*{inf}_{n\to\infty}f(n)/g(n)\geq1$ 我们现在准备好证明本节的主要技术结果

定理 6.1.在与定理 4.1 相同的条件下
$$\mathbf{Pr}(\mathcal{F})-\left(1-\mathrm{e}^{-\lambda/k}\right)^{k}\sim n\epsilon(n),$$
哪里
$$\begin{aligned}\epsilon(n)&\stackrel{def}{=}\left(\mathbf{Pr}(\|C(x)\|=0)-1+\frac{\lambda}{n}\right)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k}\\&+\left(\mathbf{Pr}(\|C(x)\|=1)-\frac{\lambda}{n}\right)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\\&+\sum_{j=2}^{k}\mathbf{Pr}(\|C(x)\|=j)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-j}.\end{aligned}$$
备注。这个结果在直觉上是令人满意的,因为它表明渐近误差项表示的假阳性概率部分本质上是 $\|C(x,z)\|>1$ 正好用于 S$ 中的一个 $x\ 和 Z 的另一个 $k-\|C(x,z)\|$ 哈希位置被“渐近”过滤器中 $S$ 的其他元素命中(即,在 $n-1\to\infty$ 的限制中),这与概率 $(1-\mathrm{e}^{-\lambda/k})^{k-\|C(x,z)\|}$ .(这几乎遵循定理 4.1。区别在于,现在 $z$ 只有 $k-\|C(x,z)\|$ 个哈希位置,而 $S-\{x\}$ 的元素各有 $k$ 个哈希位置;但是,从定理 4.1 的证明中可以清楚地看出,在这种情况下,极限假阳性概率是 $(1-\mathrm{e}^{-\lambda/k})^{k-\|C(x,z)\|}$ )

证明。我们按照与定理 4.1 证明相同的思路开始。首先,我们采用那里介绍的约定,它允许我们将 $H(z)$ 的元素(与多重性)与 $[k]$ 的元素相关联。接下来,对于 $i\in[k]$ 和 $x\in S$ ,如果 $i\in C(x)$ 和 $X_{i}(x)=0$,则定义 $X_{i}(x)=1$,否则$X_{i}\stackrel{\mathrm{def}}{=}\sum_{x\in S}X_{i}(x)$ ,以及 $X\stackrel{\mathrm{def}}{=}(X_{0},\ldots,X_{k-1})$ 。最后,我们将 $P\stackrel{\mathrm{def}}{=}(P_{0},\ldots,P_{k-1})$ 定义为 $k$ 独立 $\operatorname{Po}(\lambda/k)$ 随机变量定义的向量
$$\begin{aligned}&f(n)\stackrel{\mathrm{def}}{=}\mathbf{Pr}(\|C(x)\|=0)-1+\frac{\lambda}{n}\\&g_{i}(n)\stackrel{\mathrm{def}}{=}\mathbf{Pr}(i\in C(x),\|C(x)\|=1)-\frac{\lambda}{kn}\quad\mathrm{for}\:i\in[k]\\&h_{T}(n)\stackrel{\mathrm{def}}{=}\mathbf{Pr}(C(x)=f_{H(z)}^{-1}(T)))\quad\mathrm{for}\:T\subseteq[k]:|T|>1,\end{aligned}$$
请注意,它们都是由引理的假设$o\left(1/n\right)$。对于 $T\subseteq[k]$ ,我们现在 可以

------------------------------------------------------------------

写

$$\begin{aligned}\mathbf{Pr}\left(\bigcap_{i\in T}X_i=0\right)&=\prod_{x\in S}\mathbf{Pr}\left(\{i\in[k]:i\in C(x)\}\subseteq\overline{T}\right)\\&=\left(\mathbf{Pr}(\|C(x)\|=0)+\sum_{i\in T}\mathbf{Pr}(i\in C(x),\|C(x)\|=1)\right.\\&+\sum_{T\leq T:|T^{\prime}|>1}\mathbf{Pr}(C(x)=f_{H(z)}^{-1}(T^{\prime}))\\&=\left(1-\frac{\lambda|T|}{kn}+f(n)+\sum_{i\in T}g_i(n)+\sum_{T^{\prime}\subseteq T|T^{\prime}|>1}h_{T^{\prime}}(n)\right)^n\\&\sim\exp\left[-\frac{\lambda|T|}k+nf(n)+\sum_{i\in T}ng_i(n)+\sum_{T^{\prime}\subseteq T:|T^{\prime}|>1}nhg_i^{\prime}(n)\right]\\&=\mathrm{e}^{-\frac{\lambda|T|}k}\left(\exp\left[nf(n)+\sum_{i\in T}ng_i(n)+\sum_{T^{\prime}\subseteq T:|T^{\prime}|>1}nhg_i^{\prime}(n)\right]\right)\\&\sim\mathrm{e}^{-\frac{\lambda|T|}k}\left(1+nf(n)+\sum_{i\in T}ng_i(n)+\sum_{T^{\prime}\subseteq T:|T^{\prime}|>1}nhg_i^{\prime}(n)\right),\end{aligned}$$
前两个步骤很明显,第三个步骤来自 $f$ 的定义,$y_{i}$ 的定义,以及

$hT^{\prime}$ 的,以及第四步和第六步,都是从所有这些函数都是 $o\left(1/n\right)$ 的假设开始的(因为 $\mathrm{e}^{t(n)}\sim1+t(n)$ 如果 $t(n)=o(1)$)

因此,包含/排除原则意味着

$$\begin{aligned}(\mathcal{F})-\mathbf{Pr}(\forall i:P_{i}>0)&=-\left(\mathbf{Pr}(\exists i:X_{i}=0)-\mathbf{Pr}(\exists i:P_{i}=0)\right)\\&=-\sum_{\emptyset\subset T\subseteq[k]}(-1)^{|T|+1}\left(\mathbf{Pr}\left(\bigcap_{i\in T}X_{i}=0\right)-\mathbf{Pr}\left(\bigcap_{i\in T}P_{i}=0\right)\right)\\&=\sum_{\emptyset\subset T\subseteq[k]}(-1)^{|T|}\left(\mathbf{Pr}\left(\bigcap_{i\in T}X_{i}=0\right)-\mathrm{e}^{-\frac{\lambda|T|}{k}}\right)\\&\sim n\sum_{\emptyset\subset T\subseteq[k]}(-1)^{|T|}\mathrm{e}^{-\frac{\Delta|T|}{k}}\left(f(n)+\sum_{i\in T}g_{i}(n)+\sum_{T^{\prime}\subseteq\overline{T}:[T^{\prime}]>1}h_{T^{\prime}}(n)\right).\end{aligned}$$

------------------------------------------------------------------

求和最后一行,我们写

$$\begin{aligned}M&\stackrel{\mathrm{def}}{=}\sum_{\emptyset\subset T\subseteq[k]}(-1)^{|T|}\mathrm{e}^{-\frac{\lambda|T|}{k}}\left(f(n)+\sum_{i\in\overline{T}}g_{i}(n)+\sum_{T^{\prime}\subseteq\overline{T}:|T^{\prime}|>1}h_{T^{\prime}}(n)\right)\\&=\sum_{j=1}^{k}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\sum_{T\subseteq[k]:[T|=j}f(n)\\&+\sum_{j=1}^{k}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\sum_{T\subseteq[k]:|T|=j}\sum_{i\in\overline{T}}g_{i}(n)\\&+\sum_{j=1}^{k}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\sum_{T\subseteq[k]:|T|=j}\sum_{T\subseteq\overline{T}:|T^{\prime}|>1}h_{T^{\prime}}(n),\end{aligned}$$

并分别评估每个术语。首先,我们计算

$$\begin{aligned}\sum_{j=1}^{k}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\sum_{T\subseteq[k]:[T]=j}f(n)&=f(n)\sum_{j=1}^{k}\binom{k}{j}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\\&=\left(\mathbf{Pr}(\|C(x)\|=0)-1+\frac{\lambda}{n}\right)\left(\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k}-1\right)\end{aligned}$$

接下来,我们看到

$$\begin{aligned}\sum_{j=1}^{k}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\sum_{T\subseteq[k]:[T]=j}\sum_{i\in\overline{T}}g_{i}(n)&=\sum_{j=1}^{k}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\sum_{i\in[k]}g_{i}(n)|\:\{T\subseteq[k]\::\:|T|=j,i\not\in T\}\\&=\left(\sum_{i\in[k]}g_{i}(n)\right)\sum_{j=1}^{k}\binom{k-1}{j}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\\&=\left(\sum_{i\in[k]}g_{i}(n)\right)\left(\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}-1\right)\\&=\left(\mathbf{Pr}(\|C(x)\|=1)-\frac{\lambda}{n}\right)\left(\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}-1\right),\end{aligned}$$

其中我们使用了 $\binom{k-1}k=0$ 的约定。现在,对于最后一个项,我们计算

$$\begin{aligned}\sum_{T\subseteq[k]:|T|=j}\sum_{T^{\prime}\subseteq\overline{T}:|T^{\prime}|>1}h_{T^{\prime}}(n)&=\sum_{\ell=2}^{k-j}\sum_{T^{\prime}\subseteq[k]:|T^{\prime}|=\ell}h_{T^{\prime}}(n)|\left\{T\subseteq[k]\::\:|T|=j,T^{\prime}\subseteq\overline{T}\right\}|\\&=\sum_{\ell=2}^{k-j}\binom{k-\ell}{j}\Pr(\|C(x)\|=\ell),\end{aligned}$$

------------------------------------------------------------------

所以

$$\begin{aligned}
\sum_{j=1}^{k}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\sum_{T\subseteq[k]:[T]=j}\sum_{T^{\prime}\subseteq\overline{T}:|T^{\prime}|>1}h_{T^{\prime}}(n)& =\sum_{j=1}^{k}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\sum_{\ell=2}^{k-j}\binom{k-\ell}{j}\Pr(\|C(x)\|=\ell) \\
&=\sum_{j=1}^{k}\sum_{\ell=2}^{k-j}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\binom{k-\ell}{j}\Pr(\|C(x)\|=\ell) \\
&=\sum_{j=1}^{k}\sum_{r=j}^{k-2}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\begin{pmatrix}r\\j\end{pmatrix}\mathbf{Pr}(\|C(x)\|=k-r) \\
&=\sum_{r=1}^{k-2}\sum_{j=1}^{r}\left(-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{j}\begin{pmatrix}r\\j\end{pmatrix}\mathbf{Pr}(\|C(x)\|=k-r) \\
&=\sum_{r=1}^{k-2}\mathbf{Pr}(\|C(x)\|=k-r)\sum_{j=1}^r\begin{pmatrix}r\\j\end{pmatrix}\begin{pmatrix}-\mathrm{e}^{-\frac{\lambda}{k}}\end{pmatrix}^j \\
&=\sum_{r=1}^{k-2}\mathbf{Pr}(\|C(x)\|=k-r)\left(\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^r-1\right) \\
&=\sum_{j=2}^{k-1}\mathbf{Pr}(\|C(x)\|=j)\left(\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-j}-1\right).
\end{aligned}$$

将项相加得到
$$\begin{aligned}\text{M}&=\left(\mathbf{Pr}(\|C(x)\|=0)-1+\frac{\lambda}{n}\right)\left(1-\mathbf{e}^{-\frac{\lambda}{k}}\right)^{k}\\&+\left(\mathbf{Pr}(\|C(x)\|=1)-\frac{\lambda}{n}\right)\left(1-\mathbf{e}^{-\frac{\lambda}{k}}\right)^{k-1}\\&+\sum_{j=2}^{k-1}\mathbf{Pr}(\|C(x)\|=j)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-j}\\&-\left(\mathbf{Pr}(\|C(x)\|=0)+\mathbf{Pr}(\|C(x)\|=1)+\sum_{j=2}^{k-1}\mathbf{Pr}(\|C(x)\|=j)-1\right).\end{aligned}$$
答案是肯定的
$$-\left(\mathbf{Pr}(\|C(x)\|=0)+\mathbf{Pr}(\|C(x)\|=1)+\sum_{j=2}^{k-1}\mathbf{Pr}(\|C(x)\|=j)-1\right)=\mathbf{Pr}(\|C(x)\|=k$$
80
$$\begin{aligned}\text{M}&=\left(\mathbf{Pr}(\|C(x)\|=0)-1+\frac{\lambda}{n}\right)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k}\\&+\left(\mathbf{Pr}(\|C(x)\|=1)-\frac{\lambda}{n}\right)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\\&+\sum_{j=2}^{k-1}\mathbf{Pr}(\|C(x)\|=j)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-j}\\&+\mathbf{Pr}(\|C(x)\|=k)\\&=\epsilon(n).\end{aligned}$$

------------------------------------------------------------------

因为
$$\mathbf{Pr}(\mathcal{F})-\left(1-\mathrm{e}^{-\lambda/k}\right)^{k}=\mathbf{Pr}(\mathcal{F})-\mathbf{Pr}(\forall i:P_{i}>0)\sim nM=n\epsilon(n),$$
结果如下。

不幸的是,我们在本文中讨论的方案通常太混乱,无法普遍应用 Theo rem 6.1;值 $\mathbf{Pr}(\|C(x)\|=j$ )取决于所使用的哈希函数的具体情况。例如,范围的大小是否为质数会影响 $\mathbf{Pr}(\|C(x)\|=j]$ 0.结果可以应用于检查特定方案的案例;例如,在分区方案中,当 $m^{\prime}$ 为素数时,$\Pr(\|C(x)\|=j)=0$ 表示 $j=2,\ldots,k-1$ ,因此表达式变得很容易计算。为了获得一般结果,我们推导出了一些简单的边界,这些边界足以得出一些有趣的结论。

引理 6.1.假设条件与定理 4.1 中的条件相同。此外,假设 fo $x\in S$ ,可以定义事件 $E_{0},\ldots,E_{\ell-1}$ 使得
$$\begin{aligned}&I.\:\mathbf{Pr}(\|C(x)\|\geq1)=\mathbf{Pr}\left(\bigcup_{i\in[\ell]}E_{i}\right)\\&2.\:\sum_{i\in[\ell]}\mathbf{Pr}(E_{i})=\lambda/n\\&3.\:\mathbf{Pr}(\|C(x)\|\geq2)\leq\sum_{i<j\in[\ell]}\mathbf{Pr}(E_{i}\cap E_{j}).\\&hen\\&n\left[\mathbf{Pr}(\|C(x)\|\equiv k)-\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\left(1+\mathrm{e}^{-\frac{\lambda}{k}}\right)\sum_{i<j\in[\ell]}\mathbf{Pr}(E_{i}\cap E_{j})\right]&\leqslant\mathbf{Pr}(\mathcal{F})-\left(1-\mathrm{e}^{-\lambda/k}\right)\\&&\leqslant n\sum_{i<j\in[\ell]}\mathbf{Pr}(E_{i}\cap E_{j})\end{aligned}$$

定理中的 Proof.As

### 6.1

我们定义
$$\begin{aligned}\epsilon(n)&\stackrel{\mathrm{def}}{=}\left(\mathbf{Pr}(\|C(x)\|=0)-1+\frac{\lambda}{n}\right)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k}\\&+\left(\mathbf{Pr}(\|C(x)\|=1)-\frac{\lambda}{n}\right)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\\&+\sum_{j=2}^{k}\mathbf{Pr}(\|C(x)\|=j)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-j},\end{aligned}$$
因此
$$\mathbf{Pr}(\mathcal{F})-\left(1-\mathrm{e}^{-\lambda/k}\right)^k\sim n\epsilon(n).$$
现在
$$\begin{aligned}
&\text{M}&&\overset{\mathrm{def}}{\operatorname*{=}}\left(\mathbf{Pr}(\|C(x)\|=0)-1+\frac\lambda n\right)\left(1-\mathrm{e}^{-\frac\lambda k}\right)^k+\left(\mathbf{Pr}(\|C(x)\|=1)-\frac\lambda n\right)\left(1-\mathrm{e}^{-\frac\lambda k}\right)^{k-1} \\
&&&=\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\left(\left(\mathbf{Pr}(\|C(x)\|=0)-1+\frac{\lambda}{n}\right)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)+\left(\mathbf{Pr}(\|C(x)\|=1)-\frac{\lambda}{n}\right)\right) \\
&&&=\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\left((\mathbf{Pr}(\|C(x)\|=0)+\mathbf{Pr}(\|C(x)\|=1)-1)-\mathrm{e}^{-\frac{\lambda}{k}}\right.\left((\mathbf{Pr}(\|C(x)\|=0)-1)+(\mathbf{Pr}(\|C(x)\|=0)-1)\right) \\
&&&=\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\left(-\mathbf{Pr}(\|C(x)\|\geq2)-\mathrm{e}^{-\frac{\lambda}{k}}\left(-\mathbf{Pr}(\|C(x)\|\geq2)-\mathbf{Pr}(\|C(x)\|=1)+\frac{\lambda}{n}\right)\right) \\
&&&=-\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k}\mathbf{Pr}(\|C(x)\|\geq2)+\mathrm{e}^{-\frac{\lambda}{k}}\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\left(\mathbf{Pr}(\|C(x)\|=1)-\frac{\lambda}{n}\right)。
\end{aligned}$$

------------------------------------------------------------------

特别是,我们有 $M\leq0$,因为

$$\mathbf{Pr}(\|C(x)\|=1)\leq\mathbf{Pr}(\|C(x)\|\geq1)=\mathbf{Pr}\left(\bigcup_{i\in[\ell]}E_i\right)\leq\sum_{i\in[l]}\mathbf{Pr}(E_i)=\lambda/n.$$

因此

$$\epsilon(n)=M+\sum_{j=2}^{k}\mathbf{Pr}(\|C(x)\|=j)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-j}\leq\mathbf{Pr}(\|C(x)\|\geq2)\leq\sum_{i<j\in[\ell]}\mathbf{Pr}(E_{i}\cap E_{j})$$

在困境中建立上限对于下限,我们注意到

$$\begin{aligned}\mathbf{Pr}(\|C(x)\|=1)-\frac{\lambda}{n}&=\mathbf{Pr}(\|C(x)\|\geq1)-\mathbf{Pr}(\|C(x)\|\geq2)-\frac{\lambda}{n}\\&=\mathbf{Pr}\left(\bigcup_{i\in[t]}E_{i}\right)-\mathbf{Pr}(\|C(x)\|\geq2)-\frac{\lambda}{n}\\&\geq\sum_{i\in[t]}\mathbf{Pr}(E_{i})-\sum_{i<j\in[t]}\mathbf{Pr}(E_{i}\cap E_{j})-\mathbf{Pr}(\|C(x)\|\geq2)-\frac{\lambda}{n}\\&=-\sum_{i<j\in[t]}\mathbf{Pr}(E_{i}\cap E_{j})-\mathbf{Pr}(\|C(x)\|\geq2)\\&\geq-2\sum_{i<j\in[t]}\mathbf{Pr}(E_{i}\cap E_{j}),\end{aligned}$$

所以



$$\begin{aligned}\text{M}&=-\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k}\mathbf{Pr}(\|C(x)\|\geq2)+\mathrm{e}^{-\frac{\lambda}{k}}\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\left(\mathbf{Pr}(\|C(x)\|=1)-\frac{\lambda}{n}\right)\\&\geq-\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k}\mathbf{Pr}(\|C(x)\|\geq2)-\mathrm{e}^{-\frac{\lambda}{k}}\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}2\sum_{i<j\in[\ell]}\mathbf{Pr}(E_{i}\cap E_{j})\\&\geq-\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k}\sum_{i<j\in[\ell]}\mathbf{Pr}(E_{i}\cap E_{j})-\mathrm{e}^{-\frac{\lambda}{k}}\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}2\sum_{i<j\in[\ell]}\mathbf{Pr}(E_{i}\cap E_{j})\\&=-\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\left(1+\mathrm{e}^{-\frac{\lambda}{k}}\right)\sum_{i<j\in[\ell]}\mathbf{Pr}(E_{i}\cap E_{j}).\end{aligned}$$



因此

$$\begin{aligned}\epsilon(n)&=\sum_{j=2}^{k}\mathbf{Pr}(\|C(x)\|=j)\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-j}+M\\&\geq\mathbf{Pr}(\|C(x)\|=k)-\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\left(1+\mathrm{e}^{-\frac{\lambda}{k}}\right)\sum_{i<j\in[\ell]}\mathbf{Pr}(E_{i}\cap E_{j}),\end{aligned}$$

完成校对。

引理 6.1 很容易应用于 5.1 和 5.2 节中讨论的方案



定理 6.2.对于 Section 5.1 中讨论的分区方案

$$\frac{k^2}{c^2n}\left[1-\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\left(1+\mathrm{e}^{-\frac{\lambda}{k}}\right)\frac{k^3}{2}\right]\lesssim\mathbf{Pr}(\mathcal{F})-\left(1-\mathrm{e}^{-\lambda/k}\right)^k\:\lesssim\frac{k^5}{2c^2n}$$

证明。我们希望应用引理 6.1.To 为此,我们将 $x\ 固定在 S$ 中,对于 $i\in[k]$ ,我们将 $E_{i}$ 定义为$i\in C(x)$ 中的事件(再次,我们使用定理 4.1 证明中引入的约定,它允许我们将 $H(z)$ 的元素与 $[k]$ 的元素相关联)。然后

$$\mathbf{Pr}(\|C(x)\|\ge1)=\mathbf{Pr}\left(\bigcup_{i\in[k]}E_i\right).$$

回想一下定理 5.1 的证明,分区方案满足 Theo rem 4.1 的条件,即 $\lambda=k^{2}/c$ 。此外,(正如我们在定理 5.1 的证明中看到的那样)

$$\begin{aligned}\sum_{i\in[k]}\mathbf{Pr}(E_i)=\sum_{i\in[k]}\frac{1}{m'}=\frac{\lambda}{n}.\end{aligned}$$

定理 5.1 的证明还告诉我们,对于 $i\neq j\in[k]$

$$\mathbf{Pr}(E_i\cap E_j)\leq\frac{k}{(m')^2}=\frac{k^3}{c^2n^2},$$

所以

$$\Pr(\|C(x)\|\ge2)\le\sum_{i<j\in[k]}\mathbf{Pr}(E_i\cap E_j)\le\frac{k^5}{2c^2n^2},$$

其中我们使用了(明显的)事实,即每个 $u\in U$ 在分区方案中都分配了 $k$ 个不同的哈希位置。最后,我们注意到 $\|如果 $h_{1}(x)=h_{1}(z)$ 且 $h_{2}(x)=h_{2}(z)$ ,SC

$$\mathbf{Pr}(\|C(x)\|=k)\geq\frac{1}{(m')^2}=\frac{k^2}{c^2n^2}.$$

将这些边界代入 Lemma 6.1 的结果中会得到结果。

定理 6.3.对于 Section 5.2 中讨论的双重哈希方案。

$$\frac{1}{c^{2}n}\left[1-\left(1-\mathrm{e}^{-\frac{\lambda}{k}}\right)^{k-1}\left(1+\mathrm{e}^{-\frac{\lambda}{k}}\right)\frac{k^{5}}{2}\right]\lesssim\mathbf{Pr}(\mathcal{F})-\left(1-\mathrm{e}^{-\lambda/k}\right)^{k}\:\lesssim\frac{k^{5}}{2c^{2}n}$$

证明。我们希望应用 Lemma 6.1。首先,从定理 5.2 的证明中回想一下,每个双哈希方案都满足定理 4.1 的条件,其中 $\lambda=k^{2}/c$ 现在修复 $x\in S$ 我们重新引入了定理 5.2 证明中的一些符号。对于 $u\in U$ 和 $i\in[k]$ ,我们定义

$$g_i(u)=h_1(u)+ih_2(u)+f(i)$$

(我们继续使用约定,即所有涉及哈希函数 $h_{1}$ anc $h_{2}$ 的算术都是以 $7/l$ 为模数完成的)接下来,对于 $i,j\in[k]$ ,我们将 $E_{i,j}$ 定义为 $g_{j}(x)=g_{i}(z)$ 的事件。然后

$$\mathbf{Pr}(\|C(x)\|\ge1)=\mathbf{Pr}\left(\bigcup_{i,j\in[k]}E_{i,j}\right),$$

------------------------------------------------------------------

而且,正如我们在定理 5.2 的证明中看到的那样

$$\sum\limits_{i,j\in[k]}\mathbf{Pr}(E_{i,j})=\sum\limits_{i,j\in[k]}\mathbf{Pr}(g_j(x)=g_i(z))=\sum\limits_{i,j\in[k]}\frac{1}{m}=\frac{\lambda}{n}.$$

此外,修复 $[k]^{2}$ 上的任何排序

$$\begin{aligned}\mathbf{Pr}(\|C(x)\|\geq2)&=\mathbf{Pr}(\exists i_{1},i_{2},j_{1}, j_{2}\in[k]:\forall\ell\in\{1,2\},g_{j_{\ell}}(x)=g_{i_{\ell}}(x))\\&=\mathbf{Pr}\left(\bigcup_{(i_{1},j_{1})<(i_{2},j_{2})\in[k]^{2}}E_{i_{1},j_{1}}\cap E_{i_{2},j_{2}}\right)\\&\leq\sum_{(i_{1},j_{1})<(i_{2},j_{2})\in[k]^{2}}\mathbf{Pr}(E_{i_{1},j_{1}}\cap E_{i_{2},j_{2}}),\end{aligned}$$

所以 Lemma 6.1 的条件得到满足。为了完成证明,我们注意到对于任何 $(i_{1},j_{1}),(i_{2},j_{2})\in[k^{2}]$

$$\begin{aligned}\mathbf{Pr}(E_{i_{1},j_{1}}\cap E_{i_{2},j_{2}})&=\mathbf{Pr}(g_{j_{1}}(x)=g_{i_{1}}(z),g_{j_{2}}(x)=g_{i_{2}}(z))\\&\leq\frac{1}{m}\cdot\frac{k}{m}\\&=\frac{k}{c^{2}n^{2}},\end{aligned}$$

其中第二步的计算是在定理 5.2 的证明中完成的。因此

$$\sum_{(i_{1},j_{1})<(i_{2},j_{2})\in[k]^{2}}\Pr(E_{i_{1},j_{1}}\cap E_{i_{2},j_{2}})\leq\sum_{(i_{1},j_{1})<(i_{2},j_{2})\in[k]^{2}}\frac{k}{c^{2}n^{2}}\leq\frac{k^{5}}{2c^{2}n^{2}}.$$

最后

$$\mathbf{Pr}(\|C(x)\|=k)\geq\mathbf{Pr}(h_1(x)=h_1(z),h_2(x)=h_2(z))=\frac{1}{m^2}=\frac{1}{c^2n^2}.$$

将这些边界代入 Lemma 6.1 的结果中,得到

还有待研究定理 6.2 和 6.3 中分析的误差项在实践中是否可以忽略不计。回想一下,对于到目前为止考虑的所有方案,渐近假阳性概率为 $(1-\exp[-k/c])^{k}$ ,与标准 Bloom 滤波器相同。我们希望将这种可能性降至最低。最简单的方法是在给定过滤器大小的特定应用程序约束的情况下最大化 $t$,然后根据 $t$ 的值优化 $k$,这导致设置 $k=c\ln2$(这是布隆过滤器的标准结果,使用微积分很容易获得;例如,参见 [3l],产生 $2^{-c\ln2}$ 的渐近假阳性概率应用定理 6.2 和 6.3,我们有,对于所有检查的方案,$k$ 的这个设置会导致
$$\mathbf{Pr}(\mathcal{F})-2^{-c\ln2}\lesssim\frac{(\ln2)^{5}}{2}\frac{c^{3}}{n}\quad\mathrm{as}\:n\to\infty.$$

我们现在给出一个启发式论点,即上述误差项在实践中可以忽略不计。假设上面的渐近不等式对每 $7l.$ 成立,而不仅仅是在 $Tl\rightarrow0$ 的极限中。然后

对于任何 $\epsilon>0$
$$\begin{aligned}\Pr(\mathcal{F})-2^{-c\ln2}\geq\epsilon2^{-c\ln2}&\Rightarrow\frac{(\ln2)^5}{2}\frac{c^3}{n}\geq\epsilon2^{-c\ln2}\\&\Rightarrow\frac{(\ln2)^5}{2}\frac{c^3}{n}\geq\epsilon 2^{-c}\\&\Rightarrow2^{c+3\ln c}\geq\frac{2n\epsilon}{(\ln2)^5}\\&\Rightarrow2^{2c+1}\geq\frac{2n\epsilon}{(\ln2)^5}\\&\Rightarrow c\geq\frac{1}{2}\log_{2}\left(\frac{n\epsilon}{(\ln2)^{5}}\right).\end{aligned}$$
第一步是唯一的非严格步骤,它基于以下假设:上述渐近不等式对于每个 $TL $ 成立。第二步成立,因为 $\ln2<1$ ,第三步是简单代数,第四步是从 $3\ln c<c+1$ 对于所有 $C>0$ 的事实,第五步也是简单代数。从这个启发式论点中,我们得出结论,除非 $c\gtrsim\log_{2}n$ ,否则上面分析的渐近误差项可以忽略不计。但是,在这些情况下,使用哈希表或指纹可能比使用 Bloom 过滤器更合适(例如,请参见 [12, Section 5.5])。

## 7. 多个查询

在前面的部分中,我们分析了 $\mathbf{Pr}(\mathcal{F}(z))$ 对于一些固定 Z 和中等大小的 TI 的行为。遗憾的是,在大多数应用中,这个量并不直接相关。相反,人们通常关心 $\mathcal{F}(z)$ 出现的 $z_{1},\ldots,z_{\ell}\in U-S$ 的数量分布的某些特征。换句话说,我们关心的不是特定误报发生的概率,而是关心,例如,对 $U-S$ 元素的不同查询对过滤器的不同查询的比例,它返回假阳性。由于 $\{\mathcal{F}(z):z\in U-S\}$ 不是独立的,因此 $\mathbf{Pr}(\mathcal{F})$ 的行为本身并不直接意味着这种形式的结果。本节致力于克服这个困难 现在,很容易看出,在我们在这里分析的 schemes 中,一旦每个

$x\in S$ 已经确定,事件 $\{\mathcal{F}(z):z\in U-S\}$ 是独立的,并且以相等的概率发生。更正式地说,让 $1(\cdot)$ 表示指示函数,$\{\mathbf{1}(\mathcal{F}(z)):z\in U-S\}$ 是条件独立的,并且在给定 $\{H(x):x\in S\}$ 的情况下分布相同。因此,以 $\{H(x):x\in S\}$ 为条件,大量的经典收敛结果(例如大数定律和中心极限定理)可以应用于 $\{1(\mathcal{F}(z)):z\in U-S\}$这些观察激发了一种推导出收敛结果的通用技术

对于人们在实践中可能想要的 $\{\mathbf{1}(\mathcal{F}(z)):z\in U-S\}$。首先,我们表明,在 S 元素(即 $\{H(x):\:x\in S\}$)使用的哈希位置集上,随机变量 $\{\mathbf{1}(\mathcal{F}(z)):z\in U-S\}$ 本质上是独立的伯努利试验,成功概率为 $\operatorname*{lim}_{n\to\infty}$Pr$(\mathcal{F})$ 。从技术角度来看,此结果是本节中最重要的。接下来,我们展示如何使用该结果来证明上述经典收敛定理的对应物,这些定理在我们的设置中成立正式进行,我们从关键定义开始。

定义 7.1.考虑任何 $\{H(u):u\in U\}$ 是独立且同分布的方案。写入 $S=\{x_{1},\ldots,x_{n}\}$ .假阳性率定义为随机变量
$$R=\mathbf{Pr}(\mathcal{F}\mid H(x_1),\ldots,H(x_n)).$$

------------------------------------------------------------------

假阳性率得名于以下事实:以 $R$ 为条件,随机变量 $\{\mathbf{1}(\mathcal{F}(z)):z\in U-S\}$ 是具有共同成功概率的独立伯努利试验。$R$ .因此,对 $U-S$ 元素的大量查询对过滤器的查询返回误报的分数很可能接近 $R $ 。从这个意义上说,$R$ 虽然是一个随机变量,但其作用类似于 $\{ \mathbf{1} ( \mathcal{F} ( z) )$ : $z\in U- S\}$ 的速率 重要的是要注意,在许多关于标准 Bloom 滤波器的文献中,假

阳性率的定义并非如上。相反,该术语通常用作假阳性概率的同义词。事实上,对于标准的布隆滤波器,我们定义的两个概念之间的区别在实践中并不重要,因为如第 2 节所述,可以很容易地证明 $R.$ 非常接近 $\Pr(\mathcal{F})$,概率极高(例如,参见 [11])。事实证明,这个结果非常自然地推广到本文提出的框架,因此即使在我们非常一般的环境中,这两个概念之间的实际差异在很大程度上也并不重要。但是,证明比标准 Bloom 过滤器的情况更复杂,因此我们将非常小心地使用我们定义的术语。

定理 7.1.考虑一个引理 4.1 的条件成立的方案。此外,假设存在一些函数 $y$ 和独立的同分布随机变量。$\{V_{u}:u\in U\}$ ,使得 $V_{u}$ 在 $\operatorname{Supp}(V_{u})$ 上是均匀的,而对于 $u\in U$ ,我们有 $H(u)=g(V_{u})$定义

$$\begin{aligned}&p\stackrel{def}{=}\left(1-\mathrm{e}^{-\lambda/k}\right)^{k}\\&\Delta\stackrel{def}{=}\max_{i\in H}\mathbf{Pr}(i\in H(u))-\frac{\lambda}{nk}\quad(=o(1/n))\\&\xi\stackrel{def}{=}nk\Delta(2\lambda+k\Delta)\quad(=o(1))\end{aligned}$$

然后对于任何 $\epsilon=\epsilon(n)>0$ 和 $\epsilon=\omega(|\mathbf{Pr}(\mathcal{F})-p|)$ ,for n 足够大,使得 $\epsilon>|\Pr(\mathcal{F})-p|$

$$\mathbf{Pr}(|R-p|>\epsilon)\leq2\exp\left[\frac{-2n(\epsilon-|\mathbf{Pr}(\mathcal{F})-p|)^{2}}{\lambda^{2}+\xi}\right].$$

此外,对于任何功能。$h( n)$ = $o( \operatorname* { min} ( 1/ | \mathbf{Pr}( \mathcal{F} )$ $- p| , \sqrt {n}) )$ ,我们有 $(R-p)h(n)$ 收敛到 0 的概率为 $Tl\rightarrow\mathbf{x}$

备注。由于引理 4.1 的 $\left|\mathbf{Pr}(\mathcal{F})-p\right|=o(1)$,我们可以在定理 7.1 中采用 $h(n)=1$ 来得出结论,$R$ 收敛到 $P$ 的概率为 $Tl\to\infty$

从定理 5.1 和 5.2 的证明中,很容易看出,对于分区和(扩展的)双重哈希方案,$\Delta=0$ SO $\xi=0$ 对于这两个方案也是如此

备注。我们在 $H(u)$ 的分布上添加了一个新条件,但它在本文中讨论的所有方案中都微不足道(因为,对于独立的完全随机哈希函数 $h_{1}$ 和 $h_{2}$ ,随机变量 $\{ ( h_{1}( u) , h_{2}( u) ) :$ $u\in U\}$ 是独立的并且是同分布的, 和 $(h_{1}(u),h_{2}(u))$ 均匀分布在其支持上)。

证明。该证明本质上是将 Azuma 不等式的标准应用于适当定义的 Doob 马丁格尔。具体来说,我们采用 [12, Section 12.5] 中讨论的技术为方便起见,写 $S=\{x_{1},\ldots,x_{n}\}$ 。对于 $h_{1},\ldots,h_{n}\in$Supp$(H(u))$ ,定义

$$f(h_1,\ldots,h_n)\stackrel{\mathrm{def}}{=}\mathbf{Pr}(\mathcal{F}\mid H(x_1)=h_1,\ldots,H(x_n)=h_n),$$

并注意 $R=f(H(x_{1}),\ldots,H(x_{n}))$ 。现在考虑一些 $C$,使得对于任何$h_{1},\ldots,h_{j}$ , $h_{j}^{\prime}$ $h_{j+1},\ldots,h_{n}\in$Supp$(H(u))$

$$|f(h_1,\dots,h_n)-f(h_1,\dots,h_{j-1},h_j',h_{j+1},\dots,h_n)|\leq c.$$

由于 $H(x_{i})$ 是独立的,我们可以应用 [12, Section 12.5] 的结果来获得

$$\mathbf{Pr}(|R-\mathbf{E}[R]|\geq\delta)\leq2\mathrm{e}^{-2\delta^{2}/nc^{2}},$$

对于任何 $\delta>0$

要找到 $t$ wewrite 的小选择



$$\left.\begin{aligned}&\left|f(h_{1},\ldots,h_{n})-f(h_{1},\ldots,h_{j-1},h_{j}^{\prime},h_{j+1},\ldots,h_{n})\right|\\&=\left|\Pr(\mathcal{F}\mid H(x_{1})=h_{1},\ldots,H(x_{n})=h_{n})\right.\\&-\Pr(\mathcal{F}\mid H(x_{1})=h_{1},\ldots,H(x_{j-1})=h_{j-1},H(x_{j})=h_{j}^{\prime},H(x_{j+1})=h_{j+1},\ldots H(x_{n})\\&=\frac{\left|\left|\{v\in\mathrm{Supp}(V_u):g(v)\subseteq\bigcup_{i=1}^nh_i\}\right|-\left|\left\{v\in\mathrm{Supp}(V_u):g(v)\subseteq\bigcup_{i=1}^n\left\{\begin{array}{c}h_j^{\prime}&i=j\\h_i&i\neq j\end{array}\right.\right.\right\}\right|}{\left|\mathrm{Supp}(V_u)\right|}\\&\leq\frac{\max_{v^{\prime}\in\mathrm{Supp}(V_u)}\mid\{v\in\mathrm{Supp}(V_u):|g(v)\cap g(v^{\prime})|\geq1\}\mid}{\left|\mathrm{Supp}(V_u)\right|}\\&=\max_{M^{\prime}\in\mathrm{Supp}(H(u))}\mathbf{Pr}(|H(u)\cap M^{\prime}|\geq1),\end{aligned}\right.$$



其中第一步只是 $f$ 的定义,第二步是从 $V_{u}$ 和 $y$ 的定义开始,第三步成立,因为将 $h_{i}$ 之一更改为某个 $M^{\prime}\in\operatorname{Supp}(H(u))$ 无法更改

$$\left|\left\{v\in\operatorname{Supp}(V_u)\::\:g(v)\subseteq\bigcup\limits_{i=1}^nh_i\right\}\right|$$

bv 超过

$$\left|\left\{v\in\mathrm{Supp}(V_u)\::\:|g(v)\cap M'|\geq1\right\}\right|,$$

第四步来自 $V_{u}$ 和 $y$ 的定义

现在考虑任何固定$M^{\prime}\in\operatorname{Supp}(H(u))$ ,并设 $y_{1},\ldots,y_{|M^{\prime}|}$ 是 $M^{\prime}$ 的不同元素。回想一下 $\|M^{\prime}\|=k$ ,SO $|M^{\prime}|\leq k$ .应用一个联合 bound,我们有那个

$$\begin{aligned}\mathbf{Pr}(|H(u)\cap M'|\geq1)&=\mathbf{Pr}\left(\bigcup_{i=1}^{|M^{\prime}|}y_{i}\in H(u)\right)\\&\leq\sum_{i=1}^{|M^{\prime}|}\mathbf{Pr}(y_{i}\in H(u))\\&\leq\sum_{i=1}^{|M^{\prime}|}\frac{\lambda}{kn}+\Delta\\&\leq\frac{\lambda}{n}+k\Delta.\end{aligned}$$

因此,我们可以设置 $c=\frac{\lambda}{n}+k\Delta$ 来获取

$$\mathbf{Pr}(|R-\mathbf{E}[R]|>\delta)\leq2\exp\left[\frac{-2n\delta^2}{\lambda^2+\xi}\right],$$

对于任何 $\delta>0$ .由于 $\mathbf{E}[R]=$Pr$(\mathcal{F})$ ,我们写(对于足够大的 $Tl.$,使得 $\epsilon>|\mathbf{Pr}(\mathcal{F})-p|)$

$$\begin{aligned}\mathbf{Pr}(|R-p|>\epsilon)&\leq\mathbf{Pr}(|R-\mathbf{Pr}(\mathcal{F})|>\epsilon-|\mathbf{Pr}(\mathcal{F})-p|)\\&\leq2\exp\left[\frac{-2n(\epsilon-|\mathbf{Pr}(\mathcal{F})-p|)^2}{\lambda^2+\xi}\right].\end{aligned}$$

为了完成证明,我们看到对于任何常数 $\delta>0$

$$\mathbf{Pr}(|R-p|h(n)>\delta)=\mathbf{Pr}(|R-p|>\delta/h(n))\to0\quad\mathrm{as}\:n\to\infty,$$

其中第二步来自 $|\mathbf{Pr}(\mathcal{F})-p|=o(1/h(n))$ 的事实,因此足够大 77。,

$$\begin{aligned}\mathbf{Pr}(|R-p|>\delta/h(n))&\leq2\exp\left[\frac{-2n(\delta/h(n)-|\mathbf{Pr}(\mathcal{F})-p|)^{2}}{\lambda^{2}+\xi}\right]\\&\leq2\exp\left[-\frac{\delta^{2}}{\lambda^{2}+\xi}\cdot\frac{n}{h(n)^{2}}\right]\\&\to0\quad\mathrm{as}\:n\to\infty,\end{aligned}$$

最后一步是 $h(n)=o({\sqrt{n}})$

由于以 $R.$ 为条件,事件 $\{{\mathcal{F}}(z):z\in U-S\}$ 是独立的,并且每个事件的发生概率都$R.$ ,定理 7.1 表明 $\{\mathbf{1}(\mathcal{F}(z)):z\in U-S\}$ 本质上是独立的伯努利试验,成功概率为 $\not{\mu}$ 。下一个结果是这个想法的正式化。

引理 7.1.考虑一个定理 7.1 的条件成立的方案。设 $\mathcal{F}_{n_0}(z)$ 表示 $\mathcal{F}(z)$ 在方案与 $\eta_{l}=\eta_{0}$ 一起使用的情况下表示 $\mathcal{F}(z)$ 。同样,在 ${\boldsymbol{T}\boldsymbol{l}}={\boldsymbol{Y}\boldsymbol{l}}_{0}$ 的情况下,让 $R_{n_0}$ 表示 $R$。设 $\{X_{n}\}$ 是实值随机变量序列,其中每个变量。$X_{n}$ 可以被理解为 $\{ \mathbf{1} ( \mathcal{F} _{n}( z) ) 的某个函数 :$ $z\in U- S\}$ ,并设 $Y$ 是 $1\mathbb{R}$ 上的任何概率分布。然后对于每个$x\in\mathbb{R}$ 和 $\epsilon=\epsilon(n)>0$ 和 $\epsilon=\omega(|\mathbf{Pr}(\mathcal{F})-p|)$ ,对于足够大的 $TI$,使得 $\epsilon>\left|\Pr(\mathcal{F})-p\right|$

$$\begin{aligned}|\Pr(X_{n}\leq x)-\Pr(Y\leq x)|\leq|\Pr(X_{n}\leq x\mid|R_{n}-p|\leq\epsilon)-\Pr(Y\leq x)|\\&+2\exp\left[\frac{-2n(\epsilon-|\mathbf{Pr}(\mathcal{F})-p|)^{2}}{\lambda^{2}+\xi}\right].\end{aligned}$$

证明。证明是定理 7.1.fix any $x\in$ 18 ,并选择一些满足引理条件的 $E$。Ther.

$$\begin{aligned}{}_{n}\leq x)&=\mathbf{Pr}(X_{n}\leq x,|R_{n}-p|>\epsilon)+\mathbf{Pr}(X_{n}\leq x,|R_{n}-p|\leq\epsilon)\\&=\mathbf{Pr}(X_{n}\leq x\mid|R_{n}-p|\leq\epsilon)\\&+\mathbf{Pr}(|R_{n}-p|>\epsilon)\left[\mathbf{Pr}(X_{n}\leq x\mid|R_{n}-p|>\epsilon)-\mathbf{Pr}(X_{n}\leq x\mid|R_{n}-p|\leq\epsilon\right]\end{aligned}$$

这意味着

$$|\Pr(X_n\leq x)-\Pr(X_n\leq x\mid|R_n-p|\leq\epsilon)|\leq\Pr(|R_n-p|>\epsilon).$$

因此

$$\begin{aligned}&(X_{n}\leq x)-\mathbf{Pr}(Y\leq x)|\\&\mathbf{Pr}(X_{n}\leq x)-\mathbf{Pr}(X_{n}\leq x\mid|R_{n}-p|\leq\epsilon)|+|\mathbf{Pr}(X_{n}\leq x\mid|R_{n}-p|\leq\epsilon)-\mathbf{Pr}(Y_{n}\leq x)\\&\mathbf{Pr}(|R_{n}-p|>\epsilon)+|\mathbf{Pr}(X_{n}\leq x\mid|R_{n}-p|\leq\epsilon)-\mathbf{Pr}(Y_{n}\leq x)|,\end{aligned}$$

因此,对于足够大的 $Tl.$,使得 $\epsilon>\left|\mathbf{Pr}(\mathcal{F})-p\right|$

$$\begin{aligned}|\Pr(X_{n}\leq x)-\Pr(Y\leq x)|\leq|\Pr(X_{n}\leq x\mid|R_{n}-p|\leq\epsilon)-\Pr(Y\leq x)|\\&+2\exp\left[\frac{-2n(\epsilon-|\mathbf{Pr}(\mathcal{F})-p|)^{2}}{\lambda^{2}+\xi}\right],\end{aligned}$$

由 Theorem 7.1 提供。

通过阐述定理 7.1 和引理 7.1 的强大功能,我们用它们来证明大数强定律的版本。Hoeffding 不等式和中心极限定理。

定理 7.2.考虑一个满足定理条件的方案 7.1.设 $Z\subseteq U-S$ 是可数无限的,并写成 $Z=\{z_{1},z_{2},\ldots\}$ 。然后对于任何 $\epsilon>0$ ,对于 n 足够大,使得 $\epsilon>\left|\mathbf{Pr}(\mathcal{F})-p\right|$ ,我们有。

1.

$$\Pr\left(\lim\limits_{\ell\to\infty}\dfrac{1}{\ell}\sum\limits_{i=1}^{\ell}\mathbf{1}(\mathcal{F}_{n}(z_{i}))=R_{n}\right)=1.$$

2.对于任何 $\epsilon>0$ ,对于 TI 足够大,使得 $\epsilon>\left|\Pr(\mathcal{F})-p\right|$

$$\mathbf{Pr}\left(\left|\lim\limits_{\ell\to\infty}\dfrac{1}{\ell}\sum\limits_{i=1}^\ell\mathbf{1}(\mathcal{F}_n(z_i))-p\right|>\epsilon\right)\leq2\exp\left[\dfrac{-2n(\epsilon-|\mathbf{Pr}(\mathcal{F})-p|)^2}{\lambda^2+\xi}\right].$$

特别是,$\operatorname*{lim}_{\ell\rightarrow\infty}\frac1\ell\sum_{i=1}^\ell\mathbf{1}(\mathcal{F}_n(z_i))$ 收敛到 $P$ 的概率为 $7l\rightarrow0$

3.对于任何函数 $Q(n)$ , $\epsilon>0$ ,andn 足够大,使得 $\epsilon/2>\left|\mathbf{Pr}(\mathcal{F})-p\right|$

公关
$$\mathbf{r}\left(\left|{\frac{1}{Q(n)}}\sum_{i=1}^{Q(n)}\mathbf{1}(\mathcal{F}_{n}(z_{i}))-p\right|>\epsilon\right)\leq2\mathrm{e}^{-Q(n)\epsilon^{2}/2}+2\exp\left[{\frac{-2n(\epsilon/2-|\mathbf{Pr}(\mathcal{F})-p|)^{2}}{\lambda^{2}+\xi}}\right].$$

4.对于任何函数 $Q(n)$ 和 $\operatorname*{lim}_{n\to\infty}Q(n)=\infty$ 且 $Q(n)=o(\operatorname*{min}(1/|\mathbf{Pr}(\mathcal{F})-p|^{2},n))$

$\sum_{i=1}^{Q(n)}\frac{\mathbf{1}(\mathcal{F}_{n}(z_{i}))-p}{\sqrt{Q(n)p(1-p)}}\to$N(0,1) in itituin s $7l\rightarrow0$

备注。根据定理 6.2 和 6.3,对于第 5 节中介绍的分区和双重哈希方案,$|\mathbf{Pr}(\mathcal{F})-p|=\Theta(1/n)$。因此,对于每个方案,定理 7.2 第四部分中的条件 $Q(n)=$ $o(\operatorname*{min}(1/|\mathbf{Pr}(\mathcal{F})-p|^{2},n))$ 变为 $Q(n)=o(n)$

证明。由于给定 $R_{n}$ ,随机变量 $\{\mathbf{1}(\mathcal{F}_{n}(z)):z\in Z\}$ 是条件独立的伯努利试验,具有共同的成功概率 $R_{n}$ ,因此直接应用大数强定律会产生第一项。对于第二项,我们注意到第一项意味着

$$\lim\limits_{\ell\to\infty}\frac{1}{\ell}\sum\limits_{i=1}^{\ell}\mathbf{1}(\mathcal{F}_{n}(z_{i}))\sim R_{n}.$$

然后直接应用定理 7.1 即可得出结果。

其余两项的难度略高。但是,可以使用 Lemma 7.1 的简单应用程序来处理它们。对于第三项,定义

$$X_n\stackrel{\mathrm{def}}{=}\left|\frac{1}{Q(n)}\sum_{i=1}^{Q(n)}\mathbf{1}(\mathcal{F}_n(z_i))-p\right|.$$

和 $Y\overset{\mathrm{det}}{\operatorname*{=}}0$ 。设 =e/2 得到

$$\begin{aligned}&\mathbf{Pr}(X_{n}>\epsilon\mid|R_{n}-p|\leq\delta)\\&=\mathbf{Pr}\left(\left|\sum_{i=1}^{Q(n)}\mathbf{1}(\mathcal{F}_{n}(z_{i})))-Q(n)p\right|>Q(n)\epsilon\:\bigg|\:|R_{n}-p|\leq\delta\right)\\&\leq\mathbf{Pr}\left(\left|\sum_{i=1}^{Q(n)}\mathbf{1}(\mathcal{F}_{n}(z_{i}))-Q(n)R_{n}\right|>Q(n)\left(\epsilon-|R_{n}-p|\right)\:\bigg|\:|R_{n}-p|\leq\delta\right)\\&\leq\mathbf{Pr}\left(\left|\sum_{i=1}^{Q(n)}\mathbf{1}(\mathcal{F}_{n}(z_{i}))-Q(n)R_{n}\right|>\frac{Q(n)\epsilon}{2}\:\bigg|\:|R_{n}-p|\leq\delta\right)\\&\leq2\mathrm{e}^{-Q(n)t^{2}/2},\end{aligned}$$

其中前两个步骤是显而易见的,第三步是 $\mathbf{Pr}(\mathcal{F}_{n}\mid R_{n})=R_{n}$ 的事实,第四步是霍夫丁不等式的应用(利用以下事实:给定 $R_{n}$ $\{1(\mathcal{F}_{n}(z)):z\in Z\}$ 是独立且同分布的伯努利试验,共量成功概率为 $R_{n}$ )。

现在,由于 $\mathbf{Pr}(Y\leq\epsilon)=1$

$$\mathbf{Pr}(X_{n}\leq\epsilon\mid|R_{n}-p|\leq\delta)-\mathbf{Pr}(Y\leq\epsilon)|=\mathbf{Pr}(X_{n}>\epsilon\mid|R_{n}-p|\leq\delta)\leq2\mathrm{e}^{-Q(n)\epsilon^{2}/2}.$$

引理 7.1 的应用程序现在给出了第三项。对于第四项,我们写成

Q(n) >
$$\frac{\mathbf{1}(\mathcal{F}_n(z_i))-p}{\sqrt{Q(n)p(1-p)}}=\sqrt{\frac{R_n(1-R_n)}{p(1-p)}}\left(\sum_{i=1}^{Q(n)}\frac{\mathbf{1}(\mathcal{F}_n(z_i))-R_n}{\sqrt{Q(n)R_n(1-R_n)}}+(R_n-p)\sqrt{\frac{Q(n)}{R_n(1-R_n)}}\right)$$

根据中心极限定理,

$$\displaystyle\sum_{i=1}^{Q(n)}\frac{\mathbf{1}(\mathcal{F}_n(z_i))-R_n}{\sqrt{Q(n)R_n(1-R_n)}}\to\mathrm{N}(0,1)\quad\text{在分布中}n\to\infty,$$

因为,给定 $R_{n}$ , $\{\mathbf{1}(\mathcal{F}_{n}(z)):z\in Z\}$ 是独立且同分布的伯努利试验,具有共同的成功概率 $R_{n}$ 。此外,根据定理 7.1,$R_{n}$ 的概率收敛为 $F$,即 $Tl\rightarrow\mathbf{x}$,因此足以证明 $(R_{n}-p)\sqrt{Q(n)}$ 的概率收敛为 0,即 $\eta_{b}\rightarrow\mathbf{x}$。但是 $\sqrt{Q(n)}=o(\operatorname*{min}(1/|\mathbf{Pr}(\mathcal{F})-p|,\sqrt{n}))$ ,所以定理 7.1 的另一个应用给出了结果。L


## 8. 实验

在本节中,我们实证评估了前几节的理论结果,以 $7l$ 的小值。我们对以下具体方案感兴趣:标准的 Bloom filter 方案、分区方案、双重哈希方案和扩展的双重哈希方案,其中 $f(i)=i^{2}$ 和 $f(i)=i^{3}$ 对于 $c\in\{4,8,12,16\}$ ,我们执行以下操作。首先,计算 $k\in\{\lfloor c\ln2\rfloor,\lceil c\ln2\rceil\}$ 的值

最小化 $p=(1-\exp[-k/c])^{k}$ .接下来,对于正在考虑的每个方案,重复以下过程 10,000 次:使用指定的值 $7l$ , $C$ 实例化过滤器

------------------------------------------------------------------

![](https://storage.simpletex.cn/view/f2kCxN1hUsTBi7licEFY6ZMB8WGGCq5DF)

图 1:各种方案和参数的假阳性概率估计值。

和 $k$ ,用一组 $S$ $7L$ 个项目填充过滤器,然后查询不在 $S$ 中的 $\lceil10/p\rceil$ 元素,记录过滤器返回误报的那些查询的 $Q $ 个。然后,我们通过对所有 10,000 次试验的结果求平均值来近似该方案的假阳性概率。此外,我们将试验结果按它们在 $CQ$ 的值进行分箱,以检查 $Q$ 分布的其他特征。

结果如图 1 和图 2 所示。在图 1 中,我们看到对于 $C$ 的小值,不同的方案基本上彼此无法区分,同时具有接近 $P$ 的假阳性概率/率。这个结果特别重要,因为我们正在试验的滤波器相当小,这支持了我们的说法,即即使在空间非常有限的环境中,这些方案也很有用。然而,我们也看到,对于稍大的 $c\in\{12,16\}$ 值,分区方案不再特别有用 $7L$ 的小值,而其他方案则特别有用。这个结果并不特别令人惊讶,因为从第 6 节中我们知道,所有这些方案都不适合 $Tl.$ 的小值和 $\boldsymbol{C}$ 的大值。此外,鉴于第 2 节中的观察结果,标准 Bloom 过滤器的分区版本的性能永远不会比原始版本更好,因此我们预计分区方案最不适合这些条件。尽管如此,分区方案在某些设置中可能仍然有用,因为它大大减少了哈希函数的范围。在图 2 中,我们给出了 $n=5000$ 和 $c=8$ 的实验结果的直方图

对于分区和扩展的双重哈希方案。请注意,对于 $C$ 的这个值,优化 $k$ 的结果是 $k=6$ ,所以我们有 $p\approx0.021577$ 和 $\lceil10/p\rceil=464$ 。在每个图中,我们将结果与 $f\stackrel{\mathrm{def}}{=}10,000\phi_{464p,464p(1-p)}$ 进行比较,其中

$$\phi_{\mu,\sigma^2}(x)\stackrel{\mathrm{def}}{=}\frac{\mathrm{e}^{-(x-\mu)^2/2\sigma^2}}{\sigma\sqrt{2\pi}}$$

表示 ${\mathrm{N}}(\mu,\sigma^{2})$ 的密度函数。正如人们所料,给定定理 7.2 第四部分的中心极限定理,$f$ 为每个直方图提供了一个合理的近似值

------------------------------------------------------------------

![](https://storage.simpletex.cn/view/f0vz8laYqLtYoruwgzoQmx4I7HWoPoRa4)

图 2:$CQ$ 的分布估计($n=5000 $和 $c=8 $),与 $f $相比

## 9.   修改后的 Count-Min 草图

现在,我们提出了对 [4] 中引入的 Count-Min 草图的修改,它以类似于我们对 Bloom 过滤器的改进的方式使用更少的哈希函数,但代价是空间增加很小。我们首先回顾原始数据结构。

### 9.1 Count-Min 草图回顾

以下是对 [4] 中给出的描述的简要回顾。Count-Min 草图将更新流 $(i_t,c_t)$ 作为输入,从 $t=1$ 开始,其中每个项目 $i_{t}$ 都是宇宙 $U=\{1,\ldots,n\}$ 的成员,每个计数 $Ct$ 都是一个正数。(可以扩展到负计数;我们在这里不考虑它们是为了方便。系统在时间 $T$ 的状态由向量 $\vec{a}(T)=(a_{1}(T),\ldots,a_{n}(T))$ 给出,其中 $a_{j}(T)$ 是 $t\leq T$ 和 $i_{t}=j$ 的所有 $Ct $ 的总和。当含义明确时,我们通常会去掉 $I$。Count-Min 草图由宽度为 $w\stackrel{\mathrm{def}}{=}\left\lceil\mathrm{e}/\epsilon\right\rceil$ 和深度为 $d\stackrel{\mathrm{def}}{=}\left\lceil\ln1/\delta\right\rceil$ 的数组 Count 组成

计数$[1,1],\ldots$,计数$[d,w]$ .数组的每个条目都初始化为 $U$ 。此外,CountMin 草图使用独立于成对独立族 $H$ 中选择的 $d$ 哈希函数:$\{1,\ldots,n\}\to\{1,\ldots,w\}$ Count-Min 草图的机制非常简单。每当更新 $(i,c)$

到达时,我们将 $\mathop{\mathrm{Count}}[j,h_{j}(i)]$ 增加 $t$ 对于 $j=1,\ldots,d$ 。每当我们想要 $u_{i}$ 的估计值(称为点查询)时,我们都会计算
$$\hat{a}_i\stackrel{\mathrm{def}}{=}\min_{j=1}^d\mathrm{Count}[j,h_j(i)].$$
Count-Min 草图的基本结果是,对于每个 $\dot{\tau}$
$$\hat{a}_{i}\geq a\quad\mathrm{and}\quad\mathbf{Pr}(\hat{a}_{i}\leq a_{i}+\epsilon\|\vec{a}\|)\geq1-\delta,$$

------------------------------------------------------------------

其中 norm 是 $L_{1}$ 范数。令人惊讶的是,这个非常简单的边界允许在 Count-Min 草图上高效地实现许多复杂的估计程序。有关详细信息,请再次参考读者 [4]。

### 9.2使用更少的哈希函数

现在,我们将展示如何将本文前面讨论的布隆滤波器的改进有效地应用于 Count-Min 草图。我们的修改保留了 Count-Min 草图的所有基本功能,但将所需的成对独立哈希函数数量减少到 $2\lceil(\ln1/\delta)/(\ln1/\epsilon)\rceil$ 。我们预计,在许多情况下,$E$ 和 $\delta$ 将是相关的,因此只需要恒定数量的哈希函数;事实上,在许多此类情况下,只需要两个哈希函数。我们描述了 Count-Min 草图的一个变体,它只使用两个成对独立的哈希值

功能并保证

$$\hat{a}_{i}\geq a\quad\mathrm{and}\quad\mathbf{Pr}(\hat{a}_{i}\leq a_{i}+\epsilon\|\vec{a}\|)\geq1-\epsilon.$$

鉴于这样的结果,使用 $2\lceil(\ln1/\delta)/(\ln1/\epsilon)\rceil$ 成对独立哈希函数并实现所需的失败概率 $\delta$ 的变体是很简单的:只需构建此数据结构的 $2\lceil(\ln1/\delta)/(\ln1/\epsilon)\rceil$ 个独立副本,并始终使用其中一个副本给出的最小估计值来回答点查询。我们的变体将使用编号为 $\{0,1,\ldots,d-1\}$ 的 $d$ 个表,每个表都有正好$ub$ 个计数器

编号为 $\{0,1,\ldots,w-1\}$ ,其中 $d$ 和 $UD$ 将在后面指定。我们坚持 $ub $ 为素数。就像在原始的 Count-Min 草图中一样,我们让 $\operatorname{Count}[j,k]$ 表示第 $j 个表中第 $k$ 个计数器的值。我们选择哈希函数 $h_{1}$ 和 $h_{2}$ 独立于成对独立函数。家族 ${\mathcal{H} }: \{ 0, \ldots , n- 1\}$ $\to$ $\{ 0, 1, \ldots , w- 1\}$ ,并定义 $g_{j}( x)$ = $h_{1}( x)$ + $jh_{2}( x)$ mod $U$ for $j=0,\ldots,d-1$ 我们的数据结构的机制与原始的 Count-Min 草图相同。

每当流中发生更新 $(i,c)$ 时,我们就会将 $\mathop{\mathrm{Count}}[j,g_{j}(i)]$ 增加 $t ,对于 $j=$ $ $ 0,\ldots,d-1$ 。每当我们想要 $u_{i}$ 的估计值时,我们都会计算

$$\hat{a}_i\stackrel{\mathrm{def}}{=}\min\limits_{j=0}^{d-1}\mathrm{Count}[j,g_j(i)].$$

我们证明了以下结果:

定理 9.1.对于上述 Count-Min 草图变体。

$$\hat{a}_{i}\ge a\quad and\quad\mathbf{Pr}(\hat{a}_{i}>a_{i}+\epsilon\|\vec{a}\|)\le\frac{2}{\epsilon w^{2}}+\left(\frac{2}{\epsilon w}\right)^{d}.$$

特别是,对于 $w\geq2$e$/\epsilon$ 和 $\delta\geq\ln1/\epsilon(1-1/2\mathrm{e}^{2})$

$$\hat{a}_{i}\geq a\quad and\quad\mathbf{Pr}(\hat{a}_{i}>a_{i}+\epsilon\|\vec{a}\|)\leq\epsilon.$$

证明。修复一些项目 $i $ 。设 $A_{i}$ 是所有项目 Z(除了 $\dot{i}$ )的总数,其中 $h_{1}(z)=h_{1}(i)$ 和 $h_{2}(z)=h_{2}(i)$ 。设 $B_{j,i}$ 是 $g_{j}(i)=g_{j}(z)$ 的所有项目 Z 的总数,不包括 $i$ 和计入 $A_{i}$ 的项目 2。因此

$$\hat{a}_i=\min\limits_{j=0}^{d-1}\text{Count}[j,g_j(i)]=a_i+A_i+\min\limits_{j=0}^{d-1}B_{j,i}.$$

------------------------------------------------------------------

现在,下限紧跟在 allitems 具有非负计数的事实之后。因为所有更新都是积极的。因此,我们专注于上限,我们通过注意到
$$\mathbf{Pr}(\hat a_i\ge a_i+\epsilon\|\vec a\|)\le\mathbf{Pr}(A_i\ge\epsilon\|\vec a\|/2)+\mathbf{Pr}\left(\min\limits_{j=0}^{d-1}B_{j,i}\ge\epsilon\|\vec a\|/2\right).$$
我们首先绑定了 $A_{i}$ 。设 $1(\cdot)$ 表示指标函数,我们有
$$\mathbf{E}[A_i]=\sum\limits_{z\ne i}a_z\:\mathbf{E}[\mathbf{1}(h_1(z)=h_1(i)\wedge h_2(z)=h_2(i))]\le\sum\limits_{z\ne i}a_z/w^2\le\|\vec{a}\|/w^2,$$
其中第一步来自期望的线性,第二步来自哈希函数的定义。马尔可夫不等式现在意味着
$$\Pr(A_{i}\geq\epsilon\|\vec{a}\|/2)\leq2/\epsilon w^{2}.$$
要绑定 $\operatorname*{min}_{j=0}^{d-1}B_{j,i}$ ,我们注意到对于任何 $j\in\{0,\ldots,d-1\}$ 和 $z\neq i$
$$\begin{aligned}\mathbf{Pr}(((h_{1}(z)\neq h_{1}(i)\vee h_{2}(z)\neq h_{2}(i))\wedge g_{j}(z)=g_{j}(i))&\leq\mathbf{Pr}(g_{j}(z)=g_{j}(i))\\&=\mathbf{Pr}(h_{1}(z)=h_{1}(i)+j(h_{2}(i)-h_{2}(z))\\&=1/w,\end{aligned}$$
所以
$$\mathbf{E}[B_{j,i}]=\sum_{z\ne i}a_z\:\mathbf{E}[\mathbf{1}((h_1(z)\ne h_1(i)\vee h_2(z)\ne h_2(i)))\wedge g_j(z)=g_j(i))]\le\|\vec{a}\|/w,$$
因此,马尔可夫不等式意味着
$$\mathbf{Pr}(B_{j,i}\geq\epsilon\|\vec{a}\|/2)\leq2/\epsilon w$$
对于任意$ub$ ,这个结果不足以绑定 $\operatorname*{min}_{j=0}^{d-1}B_{j,i}.$ 但是,由于 $ub$ 是素数,每个项目 $Z$ 只能对一个 $B_{k,i}$ 做出贡献(因为如果 $g_{j}(z)=g_{j}(i)$ 对于两个 $j$ 的值,我们必须有 $h_{1}(z)=h_{1}(i)$ 和 $h_{2}(z)=h_{2}(i)$ , 在这种情况下,2 的计数不包括在任何 $B_{j,i}$ ) 中。从这个意义上说,$B_{j,i}$ 是负依赖性的 [7]。因此,对于任何 $U $ 的值
$$\mathbf{Pr}\left(\min\limits_{j=0}^{d-1}B_{j,i}\geq v\right)\leq\prod\limits_{j=0}^{d-1}\mathbf{Pr}(B_{j,i}\geq v)$$
特别是,我们有
$$\mathbf{Pr}\left(\min\limits_{j=0}^{d-1}B_{j,i}\geq\epsilon\|\vec{a}\|/2\right)\leq(2/\epsilon w)^d,$$
所以
$$\begin{aligned}\mathbf{Pr}(\hat{a}_{i}\geq a_{i}+\epsilon\|\vec{a}\|)&\leq\mathbf{Pr}(A_{i}\geq\epsilon\|\vec{a}\|/2)+\mathbf{Pr}\left(\operatorname*{min}_{j=0}B_{j},i\geq\epsilon\|\vec{a}\|/2\right)\\&\leq\frac{2}{\epsilon w^{2}}+\left(\frac{2}{\epsilon w}\right)^{d}.\end{aligned}$$
对于 $w\geq2$e$/\epsilon$ 和 $\delta\geq\ln1/\epsilon(1-1/2\mathrm{e}^{2})$ ,我们有
$$\begin{aligned}\frac{2}{\epsilon w^2}+\left(\frac{2}{\epsilon w}\right)^d\leq\epsilon/2e^2+\epsilon(1-1/2\text{e}^2)=\epsilon,\end{aligned}$$
完成校对

------------------------------------------------------------------

## 10.结论

布隆过滤器是简单的随机数据结构,在实践中非常有用。事实上,它们非常有用,以至于执行 Bloom 过滤器操作所需的时间的任何显著减少都会立即转化为许多实际应用程序的大幅加速。不幸的是,Bloom 过滤器非常简单,以至于它们没有留下太多的优化空间。本文重点介绍如何修改 Bloom 过滤器以减少它们唯一依赖的资源 -

tionally use liberally: (伪)随机性。由于唯一由 .布隆过滤器是伪随机哈希函数的构造和评估,伪随机哈希函数所需数量的任何减少都会使执行布隆过滤器操作所需的时间减少几乎相等(当然,假设布隆过滤器完全存储在内存中,因此可以非常快速地执行随机访问)我们已经证明,布隆过滤器可以只用两个伪随机哈希来实现

函数时,渐近假阳性概率没有任何增加,并且,对于具有合理参数的固定大小的 Bloom 滤波器,假阳性概率没有任何实质性增加。我们还表明,渐近假阳性概率在所有实际目的和布隆滤波器参数的合理设置中起作用,就像假阳性率一样。这个结果具有巨大的实际意义,因为标准 Bloom 滤波器的类似结果本质上是它们广泛使用的理论理由。更一般地说,我们给出了一个分析修改后的 Bloom 过滤器的通用框架,该框架

我们预计将来将用于改进我们在本文中分析的具体方案。我们还希望本文中使用的技术能够有效地应用于其他数据结构,正如我们对 Count-Min 草图的修改所证明的那样。

## 致谢

我们非常感谢 Peter Dillinger 和 Panagiotis Manolios 向我们介绍了这个问题,为我们提供了他们工作的提前副本,并提供了许多有用的讨论。



## 引用

[1] P. Billingsley. Probability and Measure, Third Edition. John Wiley & Sons, 1995.

[2] P. Bose, H. Guo, E. Kranakis, A. Maheshwari, P. Morin, J. Morrison, M. Smid, and Y Tang. On the false-positive rate of Bloom filters. Submitted. Temporary version available at http://cg.scs.carleton.ca/~morin/publications/ds/bloom-submitted.pdf [3]A. Broder and M. Mitzenmacher. Network Applications of Bloom Filters: A Survey. Internet Mathematics, to appear. Temporary version available at http: //www.eecs.harvard. edu/. ~michaelm/postscripts/tempim3.pdf [4] G. Cormode and S. Muthukrishnan. Improved Data Stream Summaries: The Count-Min Sketch and its Applications. DIMACS Technical Report 2003-20, 2003 [5]P. C. Dillinger and P. Manolios. Bloom Filters in Probabilistic Verification. FMCAD 2004, Formal Methods in Computer-Aided Design, 2004. [6]P. C. Dillinger and P. Manolios. Fast and Accurate Bitstate Verification for SPIN. SPIN 2004, 11th International SPIN Workshop on Model Checking of Software, 2004.

------------------------------------------------------------------

[7]D. P. Dubhashi and D. Ranjan. Balls and Bins: A Case Study in Negative Dependence. Random Structures and Algorithms, 13(2):99-124, 1998 [8]L.Fan, P. Cao, J. Almeida,and A. Z. Broder. Summary cache: a scalable wide-area Web cache sharing protocol. IEEE/ACM Transactions on Networking, 8(3):281-293, 2000 [9] K. Ireland and M. Rosen. A Classical Introduction to Modern Number Theory, Second Edition. Springer-Verlag, New York, 1990 [10]D. Knuth. The Art of Computer Programming, Volume 3: Sorting and Searching. AddisonWesley, Reading Massachusetts, 1973. [11] M. Mitzenmacher. Compressed Bloom Filters. IEEE/ACM Transactions on Networking. 105:613-620, 2002 [12] M. Mitzenmacher and E. Upfal. Probability and Computing: Randomized Algorithms and Probabilistic Analysis. Cambridge University Press, 2005. [13] M. V. Ramakrishna. Practical performance of Bloom filters and parallel free-text searching Communications of the ACM, 32(10):1237-1239, 1989.